"""
[PROD] Igual ao import dev, só que grava no ClickHouse de PRODUÇÃO (common.clickhouse_prod).

Dicas de uso:
  --dry-run            só mostra como ficaria, sem inserir
  --file <arquivo>     processa um arquivo só (bom pra conferir antes)
"""

import argparse
import glob
import gzip
import json
import math
import os
import sys
import time

# acha a raiz do projeto pra conseguir importar o common/
_root = os.path.dirname(os.path.abspath(__file__))
while _root != os.path.dirname(_root) and not os.path.isdir(os.path.join(_root, "common")):
    _root = os.path.dirname(_root)
sys.path.insert(0, _root)

import pandas as pd
# só o tradutor do formato do DynamoDB — roda offline, n conecta na AWS nem usa credencial
from boto3.dynamodb.types import TypeDeserializer

from common import config
from common.clickhouse_prod import get_clickhouse_client, insert_dataframe
from common.progress import load_enviados, move_to_processed, upsert_entry, write_enviados_log

CH_DATABASE = config.CH_DATABASE_PROD
PROJ_TABLE  = config.PROJECTIONS_TABLE
BATCH_ROWS  = config.PROJ_BATCH_ROWS
COMPANY_ID  = config.COMPANY_ID_PROD

# pastas separadas por ambiente — prod usa a subpasta prod (dump/prod, made/prod, output/prod)
_BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DUMP_DIR   = os.path.join(_BASE_DIR, "dump", "prod")
MADE_DIR   = os.path.join(_BASE_DIR, "made", "prod")
OUTPUT_DIR = os.path.join(_BASE_DIR, "output", "prod")
ENVIADOS_LOG = os.path.join(OUTPUT_DIR, ".enviados.log")

# colunas da tabela, na ordem do insert
COLUMNS = ["company_id", "process_id", "tag_id", "value", "ts"]

# campos que são "cabeçalho" do item, não viram tag
_NON_TAG_KEYS = {"process_id", "timestamp", "ts", "company_id", "model"}

_deserializer = TypeDeserializer()


# a tabela já existe no ClickHouse (criada fora daqui); este script só insere
OPTIMIZE_TABLE_SQL = f"OPTIMIZE TABLE {CH_DATABASE}.{PROJ_TABLE} FINAL"


def optimize_table(client):
    """Junta de vez as linhas repetidas (assim não precisa de FINAL nas consultas)."""
    print(f"Consolidando tabela: {OPTIMIZE_TABLE_SQL}")
    client.command(OPTIMIZE_TABLE_SQL)
    print("OK — tabela consolidada (queries não precisam de FINAL)\n")


def discover_files(base_dir: str, single_file: str | None) -> list[str]:
    """Lista os .gz da pasta (ou um arquivo só), ignorando os manifest do export."""
    if single_file:
        return [single_file]
    found = set(glob.glob(os.path.join(base_dir, "**", "*.json.gz"), recursive=True))
    found |= set(glob.glob(os.path.join(base_dir, "**", "*.gz"), recursive=True))
    files = [f for f in found if not os.path.basename(f).startswith("manifest-")]
    return sorted(files)


def flatten_item(item: dict) -> dict:
    """Desembrulha o JSON tipado do DynamoDB ({"S": ...}) num dict normal — só texto, sem AWS."""
    return {k: _deserializer.deserialize(v) for k, v in item.items()}


def _to_float(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f


def rows_from_flat(flat: dict) -> list[dict]:
    """Transforma um item do Dynamo em uma ou mais linhas (uma por tag)."""
    process_id = flat.get("process_id")
    raw_ts     = flat.get("timestamp", flat.get("ts"))
    if process_id is None or raw_ts is None:
        return []

    company_id = str(flat.get("company_id") or COMPANY_ID)
    process_id = str(process_id)
    ts         = str(raw_ts)

    # caso o item já venha pronto com tag_id e value
    if "tag_id" in flat and "value" in flat:
        v = _to_float(flat.get("value"))
        if v is None:
            return []
        return [{"company_id": company_id, "process_id": process_id,
                 "tag_id": str(flat["tag_id"]), "value": v, "ts": ts}]

    # caso normal: cada tag do item vira uma linha
    rows = []
    for key, val in flat.items():
        if key in _NON_TAG_KEYS:
            continue
        v = _to_float(val)
        if v is None:
            continue
        rows.append({"company_id": company_id, "process_id": process_id,
                     "tag_id": str(key), "value": v, "ts": ts})
    return rows


def read_file(path: str, limit: int | None):
    """Lê o arquivo item por item (aceita .gz ou .json) e devolve as linhas + contagens."""
    records = []
    stats = {"read": 0, "accepted": 0, "discarded": 0}

    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            stats["read"] += 1

            obj = json.loads(line)
            item = obj.get("Item", obj)   # cada linha do export vem como {"Item": {...}}
            rows = rows_from_flat(flatten_item(item))

            if not rows:
                stats["discarded"] += 1
            else:
                records.extend(rows)
                stats["accepted"] += len(rows)

            if limit is not None and stats["read"] >= limit:
                break

    return records, stats


def to_dataframe(records: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(records, columns=COLUMNS)
    if not df.empty:
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df


def main():
    parser = argparse.ArgumentParser(description="Importa Projections (.json.gz do DynamoDB) para o ClickHouse.")
    parser.add_argument("--dry-run", action="store_true", help="Lê e transforma, mostra amostra e estatísticas, NÃO insere.")
    parser.add_argument("--file", help="Processa só este arquivo .gz (valida o 1º antes dos demais).")
    parser.add_argument("--limit", type=int, help="Processa só os N primeiros itens de cada arquivo.")
    parser.add_argument("--no-optimize", action="store_true", help="Não roda OPTIMIZE TABLE ... FINAL ao fim da carga real.")
    args = parser.parse_args()

    files = discover_files(DUMP_DIR, args.file)

    print(f"\n{'='*57}")
    print(f"  Host         : {config.CH_HOST_PROD}:{config.CH_PORT_PROD}")
    print(f"  Tabela       : {CH_DATABASE}.{PROJ_TABLE}")
    print(f"  Pasta dump   : {DUMP_DIR}")
    print(f"  Pasta made   : {MADE_DIR}")
    print(f"  Arquivos     : {len(files)}")
    print(f"  Batch        : {BATCH_ROWS:,} linhas")
    print(f"  Modo         : {'DRY-RUN (sem inserir)' if args.dry_run else 'CARGA REAL'}")
    print(f"{'='*57}\n")

    client = None
    if not args.dry_run:
        client = get_clickhouse_client()
        print("Conexão com ClickHouse OK\n")

    if not files:
        print(f"Nenhum arquivo .gz novo em '{DUMP_DIR}/' — nada a processar.")
        print(f"Coloque novos arquivos exportados do DynamoDB nessa pasta e rode novamente.")
        return

    entries      = [] if args.dry_run else load_enviados(ENVIADOS_LOG)
    total_read   = 0
    total_insert = 0
    total_disc   = 0
    start_all    = time.time()

    for idx, path in enumerate(files, start=1):
        rel = os.path.relpath(path, DUMP_DIR) if not args.file else path
        print(f"[{idx}/{len(files)}] Processando arquivo {rel} ...")
        t0 = time.time()

        records, stats = read_file(path, args.limit)
        df = to_dataframe(records)

        inserted = 0
        if args.dry_run:
            sample = df.head(5)
            if not sample.empty:
                print("    amostra transformada (long):")
                for line in sample.to_string(index=False).splitlines():
                    print(f"      {line}")
        else:
            for b_start in range(0, len(df), BATCH_ROWS):
                chunk = df.iloc[b_start:b_start + BATCH_ROWS]
                insert_dataframe(client, PROJ_TABLE, chunk, BATCH_ROWS)
                inserted += len(chunk)
                print(f"    Lote de {len(chunk):,} inserido com sucesso (acumulado arquivo: {inserted:,})")

        elapsed = time.time() - t0
        print(f"    OK  itens={stats['read']:,}  linhas={stats['accepted']:,}  itens vazios={stats['discarded']:,}  ({elapsed:.1f}s)")

        total_read   += stats["read"]
        total_insert += inserted
        total_disc   += stats["discarded"]

        if not args.dry_run:
            dest = move_to_processed(path, DUMP_DIR, MADE_DIR)
            if dest:
                print(f"    movido → {os.path.relpath(dest, MADE_DIR)} (made/)")
            upsert_entry(entries, {
                "file": rel,
                "read": stats["read"],
                "inserted": inserted,
                "discarded": stats["discarded"],
                "elapsed": f"{elapsed:.1f}s",
            })
            write_enviados_log(ENVIADOS_LOG, entries)
        print()

    if not args.dry_run and total_insert > 0 and not args.no_optimize:
        optimize_table(client)

    elapsed_all = (time.time() - start_all) / 60
    print(f"{'='*57}")
    print(f"  Concluído em      : {elapsed_all:.1f} minutos")
    print(f"  Arquivos rodada   : {len(files)}")
    print(f"  Itens lidos       : {total_read:,}")
    print(f"  Linhas inseridas  : {total_insert:,}")
    print(f"  Itens vazios      : {total_disc:,}")
    if not args.dry_run:
        print(f"  Log de enviados   : {ENVIADOS_LOG}")
    print(f"{'='*57}\n")


if __name__ == "__main__":
    main()
