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
import decimal
from boto3.dynamodb.types import TypeDeserializer, DYNAMODB_CONTEXT
# alguns números vêm com +38 dígitos significativos; sem isso o boto3 estoura decimal.Rounded.
# como convertemos pra float logo depois, deixar arredondar em silêncio é seguro.
DYNAMODB_CONTEXT.traps[decimal.Inexact] = 0
DYNAMODB_CONTEXT.traps[decimal.Rounded] = 0

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


def optimize_imported_partitions(client, touched: set):
    """OPTIMIZE FINAL só nas partições (mês, company_id) que ESTE import tocou — não a tabela
    inteira. A partição é (toYYYYMM(ts), company_id), então fica barato mesmo com a tabela grande."""
    for company_id, month in sorted(touched):
        print(f"  OPTIMIZE {month} / {company_id} ... ", end="", flush=True)
        t0 = time.time()
        client.command(
            f"OPTIMIZE TABLE {CH_DATABASE}.{PROJ_TABLE} PARTITION ({int(month)}, '{company_id}') FINAL",
            settings={"alter_sync": 2},
        )
        print(f"OK ({time.time() - t0:.1f}s)")


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


def stream_records(path: str, limit: int | None, stats: dict):
    """Gera as linhas item por item (aceita .gz ou .json), sem segurar o arquivo inteiro
    na memória.

    Antes o arquivo era lido todo de uma vez (lista + DataFrame inteiros), o que estourava
    a RAM em arquivos grandes e derrubava o WSL."""
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
                stats["accepted"] += len(rows)
                yield from rows

            if limit is not None and stats["read"] >= limit:
                break


HOURS_OFFSET = 3      # Dynamo entrega o ts naive (tratado como UTC); soma 3h, igual ao ADX
ALT_HOURS_OFFSET = 4  # exceção: +4h (Manaus, UTC-4)
# process_ids (sem hífen) que usam +4h em vez de +3h
ALT_PROCESSES = {
    "668dfaa49d19463e9fb0082f8ae52c2f",
    "2e7fa9dccd574a1494a95b43ca76d5f2",
    "567b3498d5d9469681fe1586591fbd9e",
    "f0c7130974924b70a34b7b9516b1036c",
    "b2fd0a766acd40819d8e34df4ba81c48",
    "ef41b354e25948b49667f948694a0957",
}


def to_dataframe(records: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(records, columns=COLUMNS)
    if df.empty:
        return df
    # +3h em todos; +1h extra (=+4h) nos process_ids de Manaus
    df["ts"] = pd.to_datetime(df["ts"], utc=True) + pd.Timedelta(hours=HOURS_OFFSET)
    extra = ALT_HOURS_OFFSET - HOURS_OFFSET
    if extra:
        manaus = df["process_id"].astype(str).str.replace("-", "", regex=False).str.lower().isin(ALT_PROCESSES)
        df.loc[manaus, "ts"] += pd.Timedelta(hours=extra)
    return df


def main():
    parser = argparse.ArgumentParser(description="Importa Projections (.json.gz do DynamoDB) para o ClickHouse.")
    parser.add_argument("--dry-run", action="store_true", help="Lê e transforma, mostra amostra e estatísticas, NÃO insere.")
    parser.add_argument("--file", help="Processa só este arquivo .gz (valida o 1º antes dos demais).")
    parser.add_argument("--limit", type=int, help="Processa só os N primeiros itens de cada arquivo.")
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
    print(f"  Ajuste hora  : +{HOURS_OFFSET}h")
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
    touched      = set()   # pares (company_id, mês) tocados, pra OPTIMIZE só neles no fim
    start_all    = time.time()

    for idx, path in enumerate(files, start=1):
        rel = os.path.relpath(path, DUMP_DIR) if not args.file else path
        print(f"[{idx}/{len(files)}] Processando arquivo {rel} ...")
        t0 = time.time()

        stats = {"read": 0, "accepted": 0, "discarded": 0}
        inserted = 0
        sample_shown = False
        batch: list[dict] = []

        def _flush(batch_rows):
            """Converte o lote acumulado em DataFrame e insere (ou mostra amostra no dry-run)."""
            nonlocal inserted, sample_shown
            if not batch_rows:
                return
            df = to_dataframe(batch_rows)
            if args.dry_run:
                if not sample_shown:
                    sample = df.head(5)
                    if not sample.empty:
                        print("    amostra transformada (long):")
                        for sline in sample.to_string(index=False).splitlines():
                            print(f"      {sline}")
                    sample_shown = True
            else:
                insert_dataframe(client, PROJ_TABLE, df, BATCH_ROWS)
                inserted += len(df)
                touched.update(zip(df["company_id"], df["ts"].dt.strftime("%Y%m")))
                print(f"    Lote de {len(df):,} inserido com sucesso (acumulado arquivo: {inserted:,})")

        # streaming: acumula só BATCH_ROWS linhas por vez, insere e libera — memória limitada
        for row in stream_records(path, args.limit, stats):
            batch.append(row)
            if len(batch) >= BATCH_ROWS:
                _flush(batch)
                batch = []
        _flush(batch)   # resto do arquivo
        batch = []

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

    # consolida só as partições que este import tocou (não a tabela inteira)
    if not args.dry_run and touched:
        print(f"\nConsolidando {len(touched)} partição(ões) importada(s) (OPTIMIZE FINAL)...")
        try:
            optimize_imported_partitions(client, touched)
        except Exception as exc:
            print(f"  AVISO: OPTIMIZE falhou ({exc}). Os dados estão lá; rode o OPTIMIZE depois.")

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
