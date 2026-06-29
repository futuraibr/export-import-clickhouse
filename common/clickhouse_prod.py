"""Conexão com o ClickHouse de PRODUÇÃO e inserção de DataFrames em lote (vars CH_*_PROD do config)."""

import clickhouse_connect
import pandas as pd

from common import config


def get_clickhouse_client():
    """Abre a conexão com o ClickHouse de produção."""
    return clickhouse_connect.get_client(
        host=config.CH_HOST_PROD,
        port=config.CH_PORT_PROD,
        username=config.CH_USER_PROD,
        password=config.CH_PASSWORD_PROD,
        database=config.CH_DATABASE_PROD,
        secure=config.CH_SECURE_PROD,
    )


def insert_dataframe(client, table: str, df: pd.DataFrame, batch_rows: int = config.BATCH_ROWS):
    """Insere o DataFrame em lotes, pra não mandar tudo de uma vez."""
    for start in range(0, len(df), batch_rows):
        chunk = df.iloc[start:start + batch_rows]
        client.insert_df(table, chunk, database=config.CH_DATABASE_PROD)
