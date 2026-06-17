"""
Validação das leituras antes de subir pro ClickHouse, seguindo as mesmas regras
do Lambda de ingestão. Descarta três casos: valor inválido (NaN/Inf), timestamp
no futuro e timestamp velho demais.

Com build_logs=True também monta as linhas pra tabela operation_log (usado pelo
import por processo); com False, só conta os descartes.
"""

import json
import math
from datetime import datetime, timedelta, timezone

import pandas as pd

from common import config

# colunas que vão pra readings / operation_log
COLUMNS     = ["company_id", "tag_id", "value", "ts"]
LOG_COLUMNS = ["company_id", "tag_id", "process_id", "event_type", "raw_value", "detail", "ts"]


def apply_ingest_rules(
    df: pd.DataFrame,
    now_utc: datetime,
    company_id: str,
    process_id: str = "",
    build_logs: bool = False,
    max_past_hours: int = config.MAX_PAST_HOURS,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Recebe um DataFrame [ts, tag_id, value] e devolve (válidos, logs, contagens)."""
    log_rows = []
    stats    = {"bad_value": 0, "timestamp_future": 0, "timestamp_too_old": 0}

    # valor inválido (NaN ou Inf)
    mask_bad = df["value"].isna() | df["value"].apply(
        lambda v: math.isinf(v) if isinstance(v, float) else False
    )
    if build_logs and mask_bad.any():
        bad_df = df[mask_bad].copy()
        bad_df["raw_value_str"] = bad_df["value"].astype(str)

        # junta os bad_value por tag, guardando o intervalo de datas
        for tag_id, group in bad_df.groupby("tag_id"):
            timestamps = group["ts"].dropna().tolist()
            ts_min     = min(timestamps) if timestamps else now_utc
            ts_max     = max(timestamps) if timestamps else now_utc
            log_rows.append({
                "company_id": company_id,
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

    # timestamp no futuro
    mask_future = df["ts"] > now_utc
    if build_logs and mask_future.any():
        for _, row in df[mask_future].iterrows():
            log_rows.append({
                "company_id": company_id,
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

    # timestamp velho demais
    cutoff   = now_utc - timedelta(hours=max_past_hours)
    mask_old = df["ts"] < cutoff
    if build_logs and mask_old.any():
        for _, row in df[mask_old].iterrows():
            log_rows.append({
                "company_id": company_id,
                "tag_id":     row["tag_id"],
                "process_id": process_id,
                "event_type": "timestamp_too_old",
                "raw_value":  str(row["ts"]),
                "detail":     json.dumps({"data": {
                    "received_ts":    row["ts"].isoformat(),
                    "max_past_hours": max_past_hours,
                }}),
                "ts": now_utc,
            })
    stats["timestamp_too_old"] = int(mask_old.sum())
    df = df[~mask_old]

    df = df.copy()
    df["company_id"] = company_id
    df_valid = df[COLUMNS]

    df_logs = pd.DataFrame(log_rows, columns=LOG_COLUMNS) if log_rows else pd.DataFrame(columns=LOG_COLUMNS)

    stats["accepted"]  = len(df_valid)
    stats["discarded"] = stats["bad_value"] + stats["timestamp_future"] + stats["timestamp_too_old"]
    return df_valid, df_logs, stats
