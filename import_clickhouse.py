import glob
import math
import os
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

COMPANY_ID     = "a57d9b153ff144d9a2b6e7e8e3a04dc3"
CSV_GLOB       = "output/**/*.csv"
BATCH_ROWS     = 50_000
MAX_PAST_HOURS = 4320   # mesmo limite do Lambda (ingest.py:91-98)

PROGRESS_LOG = "output/.inseridos.log"
COLUMNS      = ["company_id", "tag_id", "value", "ts"]


def load_progress() -> set[str]:
    if not os.path.exists(PROGRESS_LOG):
        return set()
    with open(PROGRESS_LOG) as f:
        return {line.strip() for line in f if line.strip()}


def mark_done(csv_path: str):
    with open(PROGRESS_LOG, "a") as f:
        f.write(csv_path + "\n")


def process_csv(path: str, now_utc: datetime) -> tuple[pd.DataFrame, dict]:
    """
    Replica exatamente a lógica de _process_tag do Lambda (ingest.py:52-106):
      1. value não conversível, NaN ou Inf  → descarta (bad_value)
      2. ts no futuro                        → descarta (timestamp_future)
      3. ts mais antigo que MAX_PAST_HOURS   → descarta (timestamp_too_old)
    """
    df = pd.read_csv(path, parse_dates=["ts"])

    # ts já vem do ADX em UTC com tzinfo — garante timezone-aware UTC
    df["ts"] = df["ts"].dt.tz_convert(timezone.utc)

    total = len(df)
    stats = {"bad_value": 0, "timestamp_future": 0, "timestamp_too_old": 0}

    # 1. Valida value: remove NaN, Inf, None (equivale a ingest.py:52-67)
    mask_bad = df["value"].isna() | df["value"].apply(
        lambda v: math.isinf(v) if isinstance(v, float) else False
    )
    stats["bad_value"] = int(mask_bad.sum())
    df = df[~mask_bad]

    # 2. Remove timestamps futuros (ingest.py:81-88)
    mask_future = df["ts"] > now_utc
    stats["timestamp_future"] = int(mask_future.sum())
    df = df[~mask_future]

    # 3. Remove timestamps muito antigos (ingest.py:91-98)
    cutoff = now_utc - timedelta(hours=MAX_PAST_HOURS)
    mask_old = df["ts"] < cutoff
    stats["timestamp_too_old"] = int(mask_old.sum())
    df = df[~mask_old]

    df["company_id"] = COMPANY_ID
    df = df[COLUMNS]

    stats["accepted"] = len(df)
    stats["discarded"] = total - len(df)
    return df, stats


def insert_df(client, df: pd.DataFrame):
    for start in range(0, len(df), BATCH_ROWS):
        chunk = df.iloc[start:start + BATCH_ROWS]
        client.insert_df(CH_TABLE, chunk, database=CH_DATABASE)


def main():
    csv_files    = sorted(glob.glob(CSV_GLOB, recursive=True))
    csv_files    = [f for f in csv_files if f.endswith(".csv")]
    already_done = load_progress()
    pending      = [f for f in csv_files if f not in already_done]

    print(f"\n{'='*57}")
    print(f"  Host         : {CH_HOST}:{CH_PORT}")
    print(f"  Tabela       : {CH_DATABASE}.{CH_TABLE}")
    print(f"  CSVs total   : {len(csv_files)}")
    print(f"  Já inseridos : {len(already_done)}")
    print(f"  Pendentes    : {len(pending)}")
    print(f"  Limite idade : {MAX_PAST_HOURS}h ({MAX_PAST_HOURS // 24} dias)")
    print(f"{'='*57}\n")

    client = clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
        secure=False,
    )
    print("Conexão com ClickHouse OK\n")

    now_utc      = datetime.now(timezone.utc)
    total_accept = 0
    total_discard= 0
    failed_files = []
    start_time   = time.time()

    for idx, path in enumerate(pending, start=1):
        label = os.path.relpath(path, "output")
        print(f"[{idx:>3}/{len(pending)}] {label} ...", end=" ", flush=True)
        try:
            df, stats = process_csv(path, now_utc)

            if len(df) > 0:
                insert_df(client, df)

            mark_done(path)
            total_accept  += stats["accepted"]
            total_discard += stats["discarded"]

            elapsed = time.time() - start_time
            rate    = total_accept / elapsed if elapsed > 0 else 0

            discard_info = ""
            if stats["discarded"] > 0:
                parts = []
                if stats["bad_value"]:       parts.append(f"bad_value={stats['bad_value']}")
                if stats["timestamp_future"]: parts.append(f"futuro={stats['timestamp_future']}")
                if stats["timestamp_too_old"]:parts.append(f"antigo={stats['timestamp_too_old']}")
                discard_info = f"  descartados: {', '.join(parts)}"

            print(f"OK  aceitos={stats['accepted']:,}{discard_info}  |  total={total_accept:,}  |  {rate:,.0f} rows/s")

        except Exception as exc:
            print(f"ERRO: {exc}")
            failed_files.append(path)

    elapsed_min = (time.time() - start_time) / 60
    print(f"\n{'='*57}")
    print(f"  Concluído em    : {elapsed_min:.1f} minutos")
    print(f"  Linhas inseridas: {total_accept:,}")
    print(f"  Linhas descart. : {total_discard:,}")
    print(f"  Arquivos c/ erro: {len(failed_files)}")
    if failed_files:
        print(f"\n  Rode novamente para retentar os arquivos com erro.")
        print(f"  Progresso salvo em: {PROGRESS_LOG}")
    print(f"{'='*57}\n")


if __name__ == "__main__":
    main()
