"""
Validação das leituras antes de subir pro ClickHouse, seguindo as mesmas regras
do Lambda de ingestão. Descarta três casos: valor inválido (NaN/Inf), timestamp
no futuro e timestamp velho demais.
"""

import math
from datetime import datetime, timedelta

import pandas as pd

from common import config

# colunas que vão pra readings
COLUMNS = ["company_id", "tag_id", "value", "ts"]


def apply_ingest_rules(
    df: pd.DataFrame,
    now_utc: datetime,
    company_id: str,
    max_past_hours: int = config.MAX_PAST_HOURS,
) -> tuple[pd.DataFrame, dict]:
    """Recebe um DataFrame [ts, tag_id, value] e devolve (válidos, contagens)."""
    stats = {"bad_value": 0, "timestamp_future": 0, "timestamp_too_old": 0}

    # valor inválido (NaN ou Inf)
    mask_bad = df["value"].isna() | df["value"].apply(
        lambda v: math.isinf(v) if isinstance(v, float) else False
    )
    stats["bad_value"] = int(mask_bad.sum())
    df = df[~mask_bad]

    # timestamp no futuro
    mask_future = df["ts"] > now_utc
    stats["timestamp_future"] = int(mask_future.sum())
    df = df[~mask_future]

    # timestamp velho demais
    cutoff = now_utc - timedelta(hours=max_past_hours)
    mask_old = df["ts"] < cutoff
    stats["timestamp_too_old"] = int(mask_old.sum())
    df = df[~mask_old]

    df = df.copy()
    df["company_id"] = company_id
    df_valid = df[COLUMNS]

    stats["accepted"]  = len(df_valid)
    stats["discarded"] = stats["bad_value"] + stats["timestamp_future"] + stats["timestamp_too_old"]
    return df_valid, stats
