"""Conexão com o ClickHouse e inserção de DataFrames em lote."""

import clickhouse_connect
import pandas as pd

from common import config


def get_clickhouse_client():
    """Abre a conexão com o ClickHouse."""
    return clickhouse_connect.get_client(
        host=config.CH_HOST,
        port=config.CH_PORT,
        username=config.CH_USER,
        password=config.CH_PASSWORD,
        database=config.CH_DATABASE,
        secure=config.CH_SECURE,
    )


def insert_dataframe(client, table: str, df: pd.DataFrame, batch_rows: int = config.BATCH_ROWS):
    """Insere o DataFrame em lotes, pra não mandar tudo de uma vez."""
    for start in range(0, len(df), batch_rows):
        chunk = df.iloc[start:start + batch_rows]
        client.insert_df(table, chunk, database=config.CH_DATABASE)
