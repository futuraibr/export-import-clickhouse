"""
Sobe os CSVs do ADX (pasta dump) pro ClickHouse. Cada arquivo que entra é movido
pra pasta made, então rodar de novo só pega o que ainda falta. Se algum der erro,
ele fica em dump pra tentar na próxima vez.
"""

import glob
import os
import sys
import time
from datetime import datetime, timezone

# acha a raiz do projeto pra conseguir importar o common/
_root = os.path.dirname(os.path.abspath(__file__))
while _root != os.path.dirname(_root) and not os.path.isdir(os.path.join(_root, "common")):
    _root = os.path.dirname(_root)
sys.path.insert(0, _root)

from common import config
from common.clickhouse_prod import get_clickhouse_client, insert_dataframe
from common.progress import (
    already_processed,
    load_enviados,
    move_to_processed,
    upsert_entry,
    write_enviados_log,
)
from common.validation import apply_ingest_rules

CH_HOST     = config.CH_HOST_PROD
CH_PORT     = config.CH_PORT_PROD
CH_DATABASE = config.CH_DATABASE_PROD
CH_TABLE    = config.CH_TABLE_PROD

COMPANY_ID     = config.COMPANY_ID_PROD
BATCH_ROWS     = config.BATCH_ROWS_PROD
MAX_PAST_HOURS = config.MAX_PAST_HOURS_PROD

# pastas separadas por ambiente — prod usa a subpasta prod (dump/prod, made/prod, output/prod)
_BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
DUMP_DIR     = os.path.join(_BASE_DIR, "dump", "prod")
MADE_DIR     = os.path.join(_BASE_DIR, "made", "prod")
OUTPUT_DIR   = os.path.join(_BASE_DIR, "output", "prod")
CSV_GLOB     = os.path.join(DUMP_DIR, "**", "*.csv")
ENVIADOS_LOG = os.path.join(OUTPUT_DIR, ".enviados.log")


def process_csv(path: str, now_utc: datetime):
    """Lê o CSV e passa pela validação (tira valor inválido e timestamp fora de hora)."""
    import pandas as pd

    df = pd.read_csv(path, parse_dates=["ts"])
    df["ts"] = df["ts"].dt.tz_convert(timezone.utc)  # garante UTC

    df_valid, _, stats = apply_ingest_rules(df, now_utc, COMPANY_ID, build_logs=False)
    return df_valid, stats


def main():
    csv_files = sorted(f for f in glob.glob(CSV_GLOB, recursive=True) if f.endswith(".csv"))
    # pula o que já está em made (já foi importado antes)
    pending = [f for f in csv_files if not already_processed(f, DUMP_DIR, MADE_DIR)]

    print(f"\n{'='*57}")
    print(f"  Host         : {CH_HOST}:{CH_PORT}")
    print(f"  Tabela       : {CH_DATABASE}.{CH_TABLE}")
    print(f"  Pasta dump   : {DUMP_DIR}")
    print(f"  Pasta made   : {MADE_DIR}")
    print(f"  CSVs em dump : {len(csv_files)}")
    print(f"  A processar  : {len(pending)}")
    print(f"  Limite idade : {MAX_PAST_HOURS}h ({MAX_PAST_HOURS // 24} dias)")
    print(f"{'='*57}\n")

    if not pending:
        print(f"Nenhum CSV novo em '{DUMP_DIR}/' — nada a processar.")
        return

    client = get_clickhouse_client()
    print("Conexão com ClickHouse OK\n")

    now_utc      = datetime.now(timezone.utc)
    entries      = load_enviados(ENVIADOS_LOG)
    total_accept = 0
    total_discard= 0
    failed_files = []
    start_time   = time.time()

    for idx, path in enumerate(pending, start=1):
        label = os.path.relpath(path, DUMP_DIR)
        print(f"[{idx:>3}/{len(pending)}] {label} ...", end=" ", flush=True)
        t0 = time.time()
        try:
            df, stats = process_csv(path, now_utc)

            if len(df) > 0:
                insert_dataframe(client, CH_TABLE, df, BATCH_ROWS)

            total_accept  += stats["accepted"]
            total_discard += stats["discarded"]

            elapsed_total = time.time() - start_time
            rate = total_accept / elapsed_total if elapsed_total > 0 else 0

            discard_info = ""
            if stats["discarded"] > 0:
                parts = []
                if stats["bad_value"]:        parts.append(f"bad_value={stats['bad_value']}")
                if stats["timestamp_future"]: parts.append(f"futuro={stats['timestamp_future']}")
                if stats["timestamp_too_old"]:parts.append(f"antigo={stats['timestamp_too_old']}")
                discard_info = f"  descartados: {', '.join(parts)}"

            # deu certo → manda o arquivo pra made
            move_to_processed(path, DUMP_DIR, MADE_DIR)
            upsert_entry(entries, {
                "file": label,
                "read": stats["accepted"] + stats["discarded"],
                "inserted": stats["accepted"],
                "discarded": stats["discarded"],
                "elapsed": f"{time.time() - t0:.1f}s",
            })
            write_enviados_log(ENVIADOS_LOG, entries)

            print(f"OK  aceitos={stats['accepted']:,}{discard_info}  |  total={total_accept:,}  |  {rate:,.0f} rows/s  → made/")

        except Exception as exc:
            print(f"ERRO: {exc}")
            failed_files.append(path)

    elapsed_min = (time.time() - start_time) / 60
    print(f"\n{'='*57}")
    print(f"  Concluído em    : {elapsed_min:.1f} minutos")
    print(f"  Linhas inseridas: {total_accept:,}")
    print(f"  Linhas descart. : {total_discard:,}")
    print(f"  Arquivos c/ erro: {len(failed_files)}")
    print(f"  Log de enviados : {ENVIADOS_LOG}")
    if failed_files:
        print(f"\n  Os arquivos com erro continuam em dump/. Rode novamente para retentar.")
    print(f"{'='*57}\n")


if __name__ == "__main__":
    main()
