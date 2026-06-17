"""
Importa um processo específico do ADX (passado por argumento), pra usar quando
precisa rodar/reconferir um só. Diferente do import em lote, ele também grava os
descartes na operation_log, lê de dump e made e não move nenhum arquivo.
"""

import glob
import os
import re
import sys
import time
from datetime import datetime, timezone

# acha a raiz do projeto pra conseguir importar o common/
_root = os.path.dirname(os.path.abspath(__file__))
while _root != os.path.dirname(_root) and not os.path.isdir(os.path.join(_root, "common")):
    _root = os.path.dirname(_root)
sys.path.insert(0, _root)

import pandas as pd

from common import config
from common.clickhouse_prod import get_clickhouse_client, insert_dataframe
from common.validation import apply_ingest_rules

CH_HOST     = config.CH_HOST_PROD
CH_PORT     = config.CH_PORT_PROD
CH_DATABASE = config.CH_DATABASE_PROD
CH_TABLE    = config.CH_TABLE_PROD
CH_LOG      = config.CH_LOG_PROD

COMPANY_ID     = config.COMPANY_ID_PROD
BATCH_ROWS     = config.BATCH_ROWS_PROD
MAX_PAST_HOURS = config.MAX_PAST_HOURS_PROD

# pastas separadas por ambiente (prod usa a subpasta prod); log próprio pra não misturar com o lote
_BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
DUMP_DIR     = os.path.join(_BASE_DIR, "dump", "prod")
MADE_DIR     = os.path.join(_BASE_DIR, "made", "prod")
OUTPUT_DIR   = os.path.join(_BASE_DIR, "output", "prod")
ENVIADOS_LOG = os.path.join(OUTPUT_DIR, ".enviados_single.log")
SUMMARY_MARKER = "═" * 57


def _discover_csvs() -> list[str]:
    """Procura CSVs em dump e made (este script não move nada)."""
    found = []
    for base in (DUMP_DIR, MADE_DIR):
        found += glob.glob(os.path.join(base, "**", "*.csv"), recursive=True)
    return sorted(found)


# pega o process_id de dentro do nome do arquivo (processo_<uuid>.csv)
_UUID_RE = re.compile(r"processo_([0-9a-f\-]{32,36})", re.IGNORECASE)

def extract_process_id(path: str) -> str:
    m = _UUID_RE.search(os.path.basename(path))
    return m.group(1).replace("-", "") if m else ""


def process_csv(path: str, now_utc: datetime) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Lê o CSV e valida, já montando também as linhas de log (operation_log)."""
    process_id = extract_process_id(path)
    df = pd.read_csv(path, parse_dates=["ts"])
    df["ts"] = df["ts"].dt.tz_convert(timezone.utc)

    return apply_ingest_rules(df, now_utc, COMPANY_ID, process_id=process_id, build_logs=True)


# log próprio deste script, organizado por process_id
def _parse_log() -> list[dict]:
    if not os.path.exists(ENVIADOS_LOG):
        return []
    entries, current = [], {}
    with open(ENVIADOS_LOG) as f:
        for line in f:
            line = line.rstrip()
            if line.startswith(SUMMARY_MARKER):
                break
            if line.startswith("process_id:"):
                if current:
                    entries.append(current)
                current = {"process_id": line.split(":", 1)[1].strip(), "inserted": 0, "discarded": 0, "elapsed": ""}
            elif "Linhas inseridas:" in line:
                current["inserted"] = int(line.split(":")[1].strip().replace(",", "").replace(".", ""))
            elif "Linhas descart." in line:
                current["discarded"] = int(line.split(":")[1].strip().replace(",", "").replace(".", ""))
            elif "Concluído em" in line:
                current["elapsed"] = line.split(":")[1].strip()
    if current:
        entries.append(current)
    return entries


def _update_log(process_id: str, elapsed: float, inserted: int, discarded: int):
    existing = _parse_log()
    existing.append({"process_id": process_id.replace("-", ""), "inserted": inserted,
                     "discarded": discarded, "elapsed": f"{elapsed:.1f}s"})

    total_inserted  = sum(e["inserted"]  for e in existing)
    total_discarded = sum(e["discarded"] for e in existing)

    with open(ENVIADOS_LOG, "w") as f:
        for e in existing:
            f.write(f"process_id: {e['process_id']}\n\n")
            f.write(f"  Concluído em    : {e['elapsed']}\n")
            f.write(f"  Linhas inseridas: {e['inserted']:,}\n")
            f.write(f"  Linhas descart. : {e['discarded']:,}\n")
            f.write("\n")
        f.write(f"{SUMMARY_MARKER}\n")
        f.write(f"RESUMO TOTAL\n\n")
        f.write(f"  Process IDs ({len(existing)}):\n")
        for e in existing:
            f.write(f"    - {e['process_id']}\n")
        f.write(f"\n")
        f.write(f"  Linhas inseridas: {total_inserted:,}\n")
        f.write(f"  Linhas descart. : {total_discarded:,}\n")
        f.write(f"{SUMMARY_MARKER}\n")


def main():
    if len(sys.argv) < 2:
        print("Uso: python inserir_teste.py <process_id> [--logs-only]")
        print("Ex : python inserir_teste.py c6958968-7eb5-461a-ad52-7677bd475dc0")
        print("     python inserir_teste.py c6958968-7eb5-461a-ad52-7677bd475dc0 --logs-only")
        sys.exit(1)

    process_id = sys.argv[1].replace("-", "")
    logs_only  = "--logs-only" in sys.argv
    all_csvs   = _discover_csvs()
    csv_files  = [f for f in all_csvs if process_id in os.path.basename(f).replace("-", "")]

    if not csv_files:
        print(f"Nenhum CSV encontrado com '{process_id}' no nome.")
        sys.exit(1)

    print(f"\n{'='*57}")
    print(f"  Host       : {CH_HOST}:{CH_PORT}")
    print(f"  Tabela     : {CH_DATABASE}.{CH_TABLE}  {'[IGNORADO]' if logs_only else ''}")
    print(f"  Logs       : {CH_DATABASE}.{CH_LOG}")
    print(f"  Process ID : {process_id}")
    print(f"  CSVs       : {len(csv_files)} arquivo(s)")
    for f in csv_files:
        print(f"    - {os.path.relpath(f, _BASE_DIR)}")
    print(f"{'='*57}\n")

    client = get_clickhouse_client()
    print("Conexão com ClickHouse OK\n")

    now_utc       = datetime.now(timezone.utc)
    total_accept  = 0
    total_discard = 0
    start_time    = time.time()

    for idx, path in enumerate(csv_files, start=1):
        label = os.path.relpath(path, _BASE_DIR)
        print(f"[{idx}/{len(csv_files)}] {label} ...", end=" ", flush=True)
        try:
            df_valid, df_logs, stats = process_csv(path, now_utc)

            if len(df_valid) > 0 and not logs_only:
                insert_dataframe(client, CH_TABLE, df_valid, BATCH_ROWS)
            if len(df_logs) > 0:
                insert_dataframe(client, CH_LOG, df_logs, BATCH_ROWS)

            total_accept  += stats["accepted"]
            total_discard += stats["discarded"]

            discard_info = ""
            if stats["discarded"] > 0:
                parts = []
                if stats["bad_value"]:         parts.append(f"bad_value={stats['bad_value']}")
                if stats["timestamp_future"]:  parts.append(f"futuro={stats['timestamp_future']}")
                if stats["timestamp_too_old"]: parts.append(f"antigo={stats['timestamp_too_old']}")
                discard_info = f"  descartados: {', '.join(parts)}"

            log_info = f"  logs={len(df_logs)}" if len(df_logs) > 0 else ""
            print(f"OK  aceitos={stats['accepted']:,}{discard_info}{log_info}")
        except Exception as exc:
            print(f"ERRO: {exc}")

    elapsed = time.time() - start_time
    print(f"\n{'='*57}")
    print(f"  Concluído em    : {elapsed:.1f}s")
    print(f"  Linhas inseridas: {total_accept:,}")
    print(f"  Linhas descart. : {total_discard:,}")
    print(f"{'='*57}\n")

    _update_log(process_id, elapsed, total_accept, total_discard)


if __name__ == "__main__":
    main()
