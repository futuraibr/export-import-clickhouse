import argparse
import glob
import gzip
import json
import math
import os
import sys
import time

# company_id OBRIGATÓRIO da linha de comando — capturado ANTES de importar o common
# (o config roda load_dotenv, que injetaria o COMPANY_ID_PROD do .env). Assim só vale o
# que for passado no comando; sem isso, aborta no main().
COMPANY_ID = os.environ.get("COMPANY_ID_PROD", "").strip()

# acha a raiz do projeto pra conseguir importar o common/
_root = os.path.dirname(os.path.abspath(__file__))
while _root != os.path.dirname(_root) and not os.path.isdir(os.path.join(_root, "common")):
    _root = os.path.dirname(_root)
sys.path.insert(0, _root)

import pandas as pd
# só o tradutor do formato do DynamoDB — roda offline, não conecta na AWS nem usa credencial
import decimal
from boto3.dynamodb.types import TypeDeserializer, DYNAMODB_CONTEXT
DYNAMODB_CONTEXT.traps[decimal.Inexact] = 0
DYNAMODB_CONTEXT.traps[decimal.Rounded] = 0

from common import config
from common.clickhouse_prod import get_clickhouse_client, insert_dataframe
from common.progress import load_enviados, move_to_processed, upsert_entry, write_enviados_log

CH_DATABASE = config.CH_DATABASE_PROD
CH_TABLE    = "event_readings"
BATCH_ROWS  = config.BATCH_ROWS_PROD

# pastas separadas por ambiente — prod usa a subpasta prod (dump/prod, made/prod, output/prod)
_BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DUMP_DIR   = os.path.join(_BASE_DIR, "dump", "prod")
MADE_DIR   = os.path.join(_BASE_DIR, "made", "prod")
OUTPUT_DIR = os.path.join(_BASE_DIR, "output", "prod")
ENVIADOS_LOG = os.path.join(OUTPUT_DIR, ".enviados.log")

# colunas da tabela, na ordem do insert (sem process_id)
COLUMNS = ["company_id", "tag_id", "value", "ts"]

# campos que são "cabeçalho" do item, não viram tag
_NON_TAG_KEYS = {"process_id", "timestamp", "ts", "company_id", "model"}

HOURS_OFFSET = 3
ALT_HOURS_OFFSET = 4

ALT_PROCESSES = {
    "668dfaa49d19463e9fb0082f8ae52c2f",
    "2e7fa9dccd574a1494a95b43ca76d5f2",
    "567b3498d5d9469681fe1586591fbd9e",
    "f0c7130974924b70a34b7b9516b1036c",
    "b2fd0a766acd40819d8e34df4ba81c48",
    "ef41b354e25948b49667f948694a0957",
}

_deserializer = TypeDeserializer()


def optimize_imported_partitions(client, touched: set):
    for company_id, month in sorted(touched):
        print(f"  OPTIMIZE {month} / {company_id} ... ", end="", flush=True)
        t0 = time.time()
        client.command(
            f"OPTIMIZE TABLE {CH_DATABASE}.{CH_TABLE} PARTITION ({int(month)}, '{company_id}') FINAL",
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
    """Transforma um item do Dynamo (tags como chaves) em uma linha por tag.

    company_id vem SEMPRE da linha de comando (COMPANY_ID); o process_id do item é mantido
    só pra decidir o offset de fuso (se é 3h+ ou 4h+)."""
    process_id = flat.get("process_id")
    raw_ts     = flat.get("timestamp", flat.get("ts"))
    if process_id is None or raw_ts is None:
        return []

    process_id = str(process_id)
    ts = str(raw_ts)

    rows = []
    for key, val in flat.items():
        if key in _NON_TAG_KEYS:
            continue
        v = _to_float(val)
        if v is None:
            continue
        rows.append({"company_id": COMPANY_ID, "process_id": process_id,
                     "tag_id": str(key), "value": v, "ts": ts})
    return rows


def stream_records(path: str, limit: int | None, stats: dict):
    """Gera as linhas item por item (aceita .gz ou .json), sem segurar o arquivo na memória."""
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            stats["read"] += 1

            obj = json.loads(line)
            item = obj.get("Item", obj)
            rows = rows_from_flat(flatten_item(item))

            if not rows:
                stats["discarded"] += 1
            else:
                stats["accepted"] += len(rows)
                yield from rows

            if limit is not None and stats["read"] >= limit:
                break


def to_dataframe(records: list[dict]) -> pd.DataFrame:
    # process_id é transitório (só pra decidir o offset); não vai pro insert
    df = pd.DataFrame(records, columns=["company_id", "process_id", "tag_id", "value", "ts"])
    if df.empty:
        return df[COLUMNS]
    # +3h em todos; +1h extra (=+4h) nos process_ids com fuso Manaus
    # format="mixed": alguns ts vêm sem segundos ("2023-11-25 04:22"), outros com ("...:00")
    df["ts"] = pd.to_datetime(df["ts"], utc=True, format="mixed") + pd.Timedelta(hours=HOURS_OFFSET)
    extra = ALT_HOURS_OFFSET - HOURS_OFFSET
    if extra:
        manaus = df["process_id"].astype(str).str.replace("-", "", regex=False).str.lower().isin(ALT_PROCESSES)
        df.loc[manaus, "ts"] += pd.Timedelta(hours=extra)
    return df[COLUMNS]   # descarta process_id antes de inserir


def main():
    parser = argparse.ArgumentParser(description="Importa Variables (.json.gz do DynamoDB) para o ClickHouse (event_readings).")
    parser.add_argument("--dry-run", action="store_true", help="Lê e transforma, mostra amostra e estatísticas, NÃO insere.")
    parser.add_argument("--file", help="Processa só este arquivo .gz (valida o 1º antes dos demais).")
    parser.add_argument("--limit", type=int, help="Processa só os N primeiros itens de cada arquivo.")
    args = parser.parse_args()

    if not COMPANY_ID:
        print("ERRO: defina COMPANY_ID_PROD na linha de comando.")
        print("Ex: COMPANY_ID_PROD=e2104ac3f1ff4ea7ad471b92688bde1d python sources/dynamo/variables/import_variables_json_prod.py")
        raise SystemExit(1)

    files = discover_files(DUMP_DIR, args.file)

    print(f"\n{'='*57}")
    print(f"  Host         : {config.CH_HOST_PROD}:{config.CH_PORT_PROD}")
    print(f"  Tabela       : {CH_DATABASE}.{CH_TABLE}")
    print(f"  company_id   : {COMPANY_ID}")
    print(f"  Pasta dump   : {DUMP_DIR}")
    print(f"  Pasta made   : {MADE_DIR}")
    print(f"  Arquivos     : {len(files)}")
    print(f"  Batch        : {BATCH_ROWS:,} linhas")
    print(f"  Ajuste hora  : +{HOURS_OFFSET}h (+{ALT_HOURS_OFFSET}h em {len(ALT_PROCESSES)} processos de Manaus)")
    print(f"  Modo         : {'DRY-RUN (sem inserir)' if args.dry_run else 'CARGA REAL'}")
    print(f"{'='*57}\n")

    client = None
    if not args.dry_run:
        client = get_clickhouse_client()
        print("Conexão com ClickHouse OK\n")

    if not files:
        print(f"Nenhum arquivo .gz novo em '{DUMP_DIR}/' — nada a processar.")
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
                        print("    amostra transformada:")
                        for sline in sample.to_string(index=False).splitlines():
                            print(f"      {sline}")
                    sample_shown = True
            else:
                insert_dataframe(client, CH_TABLE, df, BATCH_ROWS)
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
