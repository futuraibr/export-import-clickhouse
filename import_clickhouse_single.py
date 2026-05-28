import glob
import json
import math
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone

import clickhouse_connect
import pandas as pd

# ──────────────────────────────────────────────
# Configuração
# ──────────────────────────────────────────────
CH_HOST     = "34.151.244.227"
CH_PORT     = 8123
CH_DATABASE = "futurai_db"
CH_USER     = "app_user"
CH_PASSWORD = "admin"
CH_TABLE    = "readings"
CH_LOG      = "operation_log"

COMPANY_ID     = "a57d9b153ff144d9a2b6e7e8e3a04dc3"
CSV_GLOB       = "output/**/*.csv"
BATCH_ROWS     = 50_000
MAX_PAST_HOURS = 4320

COLUMNS      = ["company_id", "tag_id", "value", "ts"]
LOG_COLUMNS  = ["company_id", "tag_id", "process_id", "event_type", "raw_value", "detail", "ts"]
ENVIADOS_LOG = "output/.enviados.log"
SUMMARY_MARKER = "═" * 57


# ──────────────────────────────────────────────
# Extração do process_id a partir do nome do CSV
# ──────────────────────────────────────────────
_UUID_RE = re.compile(r"processo_([0-9a-f\-]{32,36})", re.IGNORECASE)

def extract_process_id(path: str) -> str:
    m = _UUID_RE.search(os.path.basename(path))
    return m.group(1).replace("-", "") if m else ""


# ──────────────────────────────────────────────
# Processamento do CSV — replica lógica do Lambda
# ──────────────────────────────────────────────
def process_csv(path: str, now_utc: datetime) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Retorna (df_valid, df_logs, stats).
    Replica exatamente _process_tag do Lambda (ingest.py:52-106).
    """
    process_id = extract_process_id(path)
    df = pd.read_csv(path, parse_dates=["ts"])
    df["ts"] = df["ts"].dt.tz_convert(timezone.utc)

    log_rows = []
    stats    = {"bad_value": 0, "timestamp_future": 0, "timestamp_too_old": 0}

    # ── 1. bad_value: NaN ou Inf ────────────────────────────────────────
    mask_bad = df["value"].isna() | df["value"].apply(
        lambda v: math.isinf(v) if isinstance(v, float) else False
    )
    if mask_bad.any():
        bad_df = df[mask_bad].copy()
        bad_df["raw_value_str"] = bad_df["value"].astype(str)

        # Agrupa por tag_id (replica consolidação ingest.py:152-171)
        for tag_id, group in bad_df.groupby("tag_id"):
            timestamps = group["ts"].dropna().tolist()
            ts_min     = min(timestamps) if timestamps else now_utc
            ts_max     = max(timestamps) if timestamps else now_utc
            log_rows.append({
                "company_id": COMPANY_ID,
                "tag_id":     tag_id,
                "process_id": process_id,
                "event_type": "bad_value",
                "raw_value":  group["raw_value_str"].iloc[0],
                "detail":     json.dumps({"data": {
                    "start_date": ts_min.isoformat(),
                    "end_date":   ts_max.isoformat(),
                }}),
                "ts": ts_min,
            })
        stats["bad_value"] = int(mask_bad.sum())
    df = df[~mask_bad]

    # ── 2. timestamp_future ─────────────────────────────────────────────
    mask_future = df["ts"] > now_utc
    if mask_future.any():
        for _, row in df[mask_future].iterrows():
            log_rows.append({
                "company_id": COMPANY_ID,
                "tag_id":     row["tag_id"],
                "process_id": process_id,
                "event_type": "timestamp_future",
                "raw_value":  str(row["ts"]),
                "detail":     json.dumps({"data": {
                    "received_ts": row["ts"].isoformat(),
                    "now":         now_utc.isoformat(),
                }}),
                "ts": now_utc,
            })
        stats["timestamp_future"] = int(mask_future.sum())
    df = df[~mask_future]

    # ── 3. timestamp_too_old ────────────────────────────────────────────
    cutoff   = now_utc - timedelta(hours=MAX_PAST_HOURS)
    mask_old = df["ts"] < cutoff
    if mask_old.any():
        for _, row in df[mask_old].iterrows():
            log_rows.append({
                "company_id": COMPANY_ID,
                "tag_id":     row["tag_id"],
                "process_id": process_id,
                "event_type": "timestamp_too_old",
                "raw_value":  str(row["ts"]),
                "detail":     json.dumps({"data": {
                    "received_ts":    row["ts"].isoformat(),
                    "max_past_hours": MAX_PAST_HOURS,
                }}),
                "ts": now_utc,
            })
        stats["timestamp_too_old"] = int(mask_old.sum())
    df = df[~mask_old]

    df["company_id"] = COMPANY_ID
    df_valid = df[COLUMNS]

    df_logs = pd.DataFrame(log_rows, columns=LOG_COLUMNS) if log_rows else pd.DataFrame(columns=LOG_COLUMNS)

    stats["accepted"] = len(df_valid)
    stats["discarded"] = stats["bad_value"] + stats["timestamp_future"] + stats["timestamp_too_old"]
    return df_valid, df_logs, stats


# ──────────────────────────────────────────────
# Inserção no ClickHouse
# ──────────────────────────────────────────────
def insert_df(client, table: str, df: pd.DataFrame):
    for start in range(0, len(df), BATCH_ROWS):
        chunk = df.iloc[start:start + BATCH_ROWS]
        client.insert_df(table, chunk, database=CH_DATABASE)


# ──────────────────────────────────────────────
# Log de progresso (.enviados.log)
# ──────────────────────────────────────────────
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


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
def main():
    if len(sys.argv) < 2:
        print("Uso: python inserir_teste.py <process_id> [--logs-only]")
        print("Ex : python inserir_teste.py c6958968-7eb5-461a-ad52-7677bd475dc0")
        print("     python inserir_teste.py c6958968-7eb5-461a-ad52-7677bd475dc0 --logs-only")
        sys.exit(1)

    process_id = sys.argv[1].replace("-", "")
    logs_only  = "--logs-only" in sys.argv
    all_csvs   = sorted(glob.glob(CSV_GLOB, recursive=True))
    csv_files  = [f for f in all_csvs if f.endswith(".csv") and process_id in os.path.basename(f).replace("-", "")]

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
        print(f"    - {os.path.relpath(f, 'output')}")
    print(f"{'='*57}\n")

    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASSWORD,
        database=CH_DATABASE, secure=False,
    )
    print("Conexão com ClickHouse OK\n")

    now_utc       = datetime.now(timezone.utc)
    total_accept  = 0
    total_discard = 0
    start_time    = time.time()

    for idx, path in enumerate(csv_files, start=1):
        label = os.path.relpath(path, "output")
        print(f"[{idx}/{len(csv_files)}] {label} ...", end=" ", flush=True)
        try:
            df_valid, df_logs, stats = process_csv(path, now_utc)

            if len(df_valid) > 0 and not logs_only:
                insert_df(client, CH_TABLE, df_valid)
            if len(df_logs) > 0:
                insert_df(client, CH_LOG, df_logs)

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
