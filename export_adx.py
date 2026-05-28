import argparse
import os
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import pandas as pd
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.helpers import dataframe_from_result_table

# ──────────────────────────────────────────────
# Configuração
# ──────────────────────────────────────────────
TEST_MODE = False

CLUSTER_URI = "https://futurai.brazilsouth.kusto.windows.net"
DATABASE    = "variables_dev"
TABLE       = "variables_a57d9b153ff144d9a2b6e7e8e3a04dc3"
OUTPUT_DIR  = "output"
DAYS_BACK   = 180

# Limite seguro abaixo do teto de 500k do ADX
CHUNK_LIMIT = 400_000

TEST_PROCESS_ID = "c1d85a0a3619494580b013a63c3899b7"

TOO_LARGE_ERROR = "E_QUERY_RESULT_SET_TOO_LARGE"


def build_client() -> KustoClient:
    kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(CLUSTER_URI)
    return KustoClient(kcsb)


def fetch_process_ids(client: KustoClient) -> list[str]:
    query = f"{TABLE} | distinct processId"
    response = client.execute(DATABASE, query)
    df = dataframe_from_result_table(response.primary_results[0])
    return df["processId"].tolist()


def count_process_rows(client: KustoClient, process_id: str) -> int:
    query = f"""
{TABLE}
| where processId == '{process_id}'
| where timestamp >= ago({DAYS_BACK}d)
| count
"""
    response = client.execute(DATABASE, query)
    df = dataframe_from_result_table(response.primary_results[0])
    return int(df["Count"].iloc[0])


def get_months_range() -> list[tuple[datetime, datetime, str]]:
    now = datetime.now(timezone.utc)
    earliest = now - relativedelta(days=DAYS_BACK)
    current = earliest.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    months = []
    while current <= now:
        next_month = current + relativedelta(months=1)
        label = current.strftime("%Y-%m")
        months.append((current, next_month, label))
        current = next_month

    return months


def _execute_range_query(client: KustoClient, process_id: str, start: datetime, end: datetime, source_timezone: str) -> pd.DataFrame:
    start_str = start.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = end.strftime("%Y-%m-%dT%H:%M:%SZ")
    query = f"""
{TABLE}
| where processId == '{process_id}'
| where timestamp >= datetime({start_str}) and timestamp < datetime({end_str})
| project ts = timestamp, tag_id = tagId, value
"""
    response = client.execute(DATABASE, query)
    df = dataframe_from_result_table(response.primary_results[0])

    if not df.empty:
        tz_source = ZoneInfo(source_timezone)
        if df["ts"].dt.tz is None:
            df["ts"] = df["ts"].dt.tz_localize(tz_source)
        df["ts"] = df["ts"].dt.tz_convert(timezone.utc)

    return df


def fetch_chunks(client: KustoClient, process_id: str, start: datetime, end: datetime, source_timezone: str) -> list[pd.DataFrame]:
    """
    Tenta buscar o intervalo inteiro. Se o ADX recusar por excesso de linhas,
    divide o intervalo ao meio e tenta cada metade recursivamente.
    Retorna uma lista de DataFrames, cada um abaixo do limite do ADX.
    """
    try:
        df = _execute_range_query(client, process_id, start, end, source_timezone)
        if len(df) == 0:
            return []
        return [df]
    except Exception as exc:
        if TOO_LARGE_ERROR in str(exc):
            mid = start + (end - start) / 2
            left = fetch_chunks(client, process_id, start, mid, source_timezone)
            right = fetch_chunks(client, process_id, mid, end, source_timezone)
            return left + right
        raise


def save_chunks(chunks: list[pd.DataFrame], month_dir: str, process_id: str) -> tuple[int, int]:
    """
    Salva os chunks em disco.
    - 1 chunk  → processo_{id}.csv
    - N chunks → processo_{id}-parte1.csv, parte2.csv, ...
    Retorna (total de linhas salvas, número de arquivos).
    """
    os.makedirs(month_dir, exist_ok=True)
    total_rows = sum(len(c) for c in chunks)

    if len(chunks) == 1:
        path = os.path.join(month_dir, f"processo_{process_id}.csv")
        chunks[0].to_csv(path, index=False)
    else:
        for i, chunk in enumerate(chunks, start=1):
            path = os.path.join(month_dir, f"processo_{process_id}-parte{i}.csv")
            chunk.to_csv(path, index=False)

    return total_rows, len(chunks)


def main():
    parser = argparse.ArgumentParser(description="Exporta dados do ADX para CSV em UTC.")
    parser.add_argument(
        "--timezone",
        default="America/Sao_Paulo",
        help="Timezone de origem dos timestamps no ADX (default: America/Sao_Paulo)",
    )
    args = parser.parse_args()

    try:
        ZoneInfo(args.timezone)
    except ZoneInfoNotFoundError:
        print(f"Timezone inválido: '{args.timezone}'. Exemplo válido: America/Sao_Paulo, UTC, Europe/London")
        raise SystemExit(1)

    source_timezone = args.timezone

    mode_label = "ATIVADO" if TEST_MODE else "DESATIVADO"
    print(f"\n{'='*57}")
    print(f"  Modo de Teste : {mode_label}")
    print(f"  Cluster       : {CLUSTER_URI}")
    print(f"  Database      : {DATABASE}")
    print(f"  Janela        : últimos {DAYS_BACK} dias")
    print(f"  Timezone      : {source_timezone} → UTC")
    print(f"  Limite/chunk  : {CHUNK_LIMIT:,} linhas")
    print(f"{'='*57}\n")

    client = build_client()

    if TEST_MODE:
        process_ids = [TEST_PROCESS_ID]
    else:
        print("Buscando IDs de processos no ADX...")
        process_ids = fetch_process_ids(client)

    months = get_months_range()
    total = len(process_ids)

    print(f"Total de processos : {total}")
    print(f"Meses a processar  : {[m[2] for m in months]}\n")

    skipped = 0
    errors = 0

    for idx, process_id in enumerate(process_ids, start=1):
        print(f"[{idx}/{total}] {process_id}")

        try:
            total_rows = count_process_rows(client, process_id)
        except Exception as exc:
            print(f"        ERRO ao contar linhas [{type(exc).__name__}]: {exc}")
            errors += 1
            continue

        if total_rows == 0:
            print(f"        IGNORADO — 0 linhas nos últimos {DAYS_BACK}d")
            skipped += 1
            continue

        print(f"        {total_rows:,} linhas no total — extraindo por mês...")

        for start, end, label in months:
            try:
                chunks = fetch_chunks(client, process_id, start, end, source_timezone)

                if not chunks:
                    print(f"        [{label}] sem dados, pulando")
                    continue

                month_dir = os.path.join(OUTPUT_DIR, label)
                saved_rows, n_parts = save_chunks(chunks, month_dir, process_id)

                if n_parts == 1:
                    path = os.path.join(month_dir, f"processo_{process_id}.csv")
                    print(f"        [{label}] OK  ({saved_rows:,} linhas)  →  {path}")
                else:
                    print(f"        [{label}] OK  ({saved_rows:,} linhas em {n_parts} partes)  →  {month_dir}/processo_{process_id}-parteN.csv")

            except Exception as exc:
                print(f"        [{label}] ERRO [{type(exc).__name__}]: {exc}")
                errors += 1

    print(f"\n{'='*57}")
    print(f"  Extração concluída.")
    print(f"  Ignorados (0 linhas) : {skipped}")
    print(f"  Erros                : {errors}")
    print(f"  Saída                : ./{OUTPUT_DIR}/")
    print(f"{'='*57}\n")


if __name__ == "__main__":
    main()
