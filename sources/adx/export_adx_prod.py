"""[PROD] Exporta dados do ADX de PRODUÇÃO (variáveis ADX_*_PROD do .env) para CSV.

O ADX entrega o timestamp em UTC; somamos +3h antes de gravar, que é o que o sistema espera.
O CSV mantém o rótulo +00:00 só pra o import preservar o valor sem deslocar.

Antes de rodar, preencha no .env: ADX_CLUSTER_URI_PROD, ADX_DATABASE_PROD, ADX_TABLE_PROD.
Depois rode o import _prod pra subir no ClickHouse de prod.
"""

import glob
import os
import sys
import time
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import pandas as pd
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.helpers import dataframe_from_result_table

# acha a raiz do projeto pra conseguir importar o common/
_root = os.path.dirname(os.path.abspath(__file__))
while _root != os.path.dirname(_root) and not os.path.isdir(os.path.join(_root, "common")):
    _root = os.path.dirname(_root)
sys.path.insert(0, _root)

from common import config

CLUSTER_URI = config.ADX_CLUSTER_URI_PROD
DATABASE    = config.ADX_DATABASE_PROD
TABLE       = config.ADX_TABLE_PROD
DUMP_DIR    = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dump", "prod")  # onde os CSVs são salvos
MADE_DIR    = os.path.join(os.path.dirname(os.path.abspath(__file__)), "made", "prod")  # pra onde o import move
DAYS_BACK   = config.DAYS_BACK_PROD

HOURS_OFFSET = 3      # padrão: ADX (UTC) +3h
ALT_HOURS_OFFSET = 4  # exceção: +4h (Manaus, UTC-4)

ALT_PROCESSES = {
    "668dfaa49d19463e9fb0082f8ae52c2f",
    "2e7fa9dccd574a1494a95b43ca76d5f2",
    "567b3498d5d9469681fe1586591fbd9e",
    "f0c7130974924b70a34b7b9516b1036c",
    "b2fd0a766acd40819d8e34df4ba81c48",
    "ef41b354e25948b49667f948694a0957",
}
TOO_LARGE_ERROR = "E_QUERY_RESULT_SET_TOO_LARGE"
MAX_RETRIES = 6   # tentativas quando o ADX limita (429 throttling)


def build_client() -> KustoClient:
    kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(CLUSTER_URI)
    return KustoClient(kcsb)


def execute_kusto(client: KustoClient, query: str):
    # Executa a query no ADX; se vier throttling (429), espera e tenta de novo (backoff)
    for attempt in range(MAX_RETRIES):
        try:
            return client.execute(DATABASE, query)
        except Exception as exc:
            throttled = "throttl" in str(exc).lower() or "429" in str(exc)
            if throttled and attempt < MAX_RETRIES - 1:
                wait = min(60, 10 * 2 ** attempt)   # 10s, 20s, 40s, 60s...
                print(f"        throttled (429) — esperando {wait}s e tentando de novo ({attempt + 1}/{MAX_RETRIES - 1})...", flush=True)
                time.sleep(wait)
                continue
            raise


def offset_for(process_id: str) -> int:
    # +4h pros processos de Manaus (ALT_PROCESSES), +3h pro resto
    return ALT_HOURS_OFFSET if str(process_id).replace("-", "").lower() in ALT_PROCESSES else HOURS_OFFSET


def shift_timestamp(ts: pd.Series, hours: int) -> pd.Series:
    # Soma `hours` ao timestamp do ADX que vem em UTC
    ts = ts.dt.tz_localize(timezone.utc) if ts.dt.tz is None else ts.dt.tz_convert(timezone.utc)
    return ts + pd.Timedelta(hours=hours)


def fetch_process_ids(client: KustoClient) -> list[str]:
    response = execute_kusto(client, f"{TABLE} | distinct processId")
    ids = dataframe_from_result_table(response.primary_results[0])["processId"].dropna().tolist()
    return [str(p) for p in ids]   # ignora nulos e garante string (alguns vêm como float/NaN)


def count_process_rows(client: KustoClient, process_id: str) -> int:
    query = f"{TABLE} | where processId == '{process_id}' | where timestamp >= ago({DAYS_BACK}d) | count"
    response = execute_kusto(client, query)
    return int(dataframe_from_result_table(response.primary_results[0])["Count"].iloc[0])


def get_months_range() -> list[tuple[datetime, datetime, str]]:
    now = datetime.now(timezone.utc)
    current = (now - relativedelta(days=DAYS_BACK)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    months = []
    while current <= now:
        next_month = current + relativedelta(months=1)
        months.append((current, next_month, current.strftime("%Y-%m")))
        current = next_month
    return months


def _execute_range_query(client: KustoClient, process_id: str, start: datetime, end: datetime) -> pd.DataFrame:
    start_str = start.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = end.strftime("%Y-%m-%dT%H:%M:%SZ")
    query = f"""
{TABLE}
| where processId == '{process_id}'
| where timestamp >= datetime({start_str}) and timestamp < datetime({end_str})
| project ts = timestamp, tag_id = tagId, value
"""
    response = execute_kusto(client, query)
    df = dataframe_from_result_table(response.primary_results[0])

    if not df.empty:
        df["ts"] = shift_timestamp(df["ts"], offset_for(process_id))

    return df


def fetch_chunks(client: KustoClient, process_id: str, start: datetime, end: datetime) -> list[pd.DataFrame]:
    """Busca o período; se o ADX reclamar que tem linha demais, parte no meio e tenta de novo."""
    try:
        df = _execute_range_query(client, process_id, start, end)
        return [df] if len(df) > 0 else []
    except Exception as exc:
        if TOO_LARGE_ERROR in str(exc):
            mid = start + (end - start) / 2
            return fetch_chunks(client, process_id, start, mid) + \
                   fetch_chunks(client, process_id, mid, end)
        raise


def save_chunks(chunks: list[pd.DataFrame], month_dir: str, process_id: str) -> tuple[int, int]:
    """Salva em CSV: um arquivo só, ou vários (-parte1, -parte2...) quando vem dividido."""
    os.makedirs(month_dir, exist_ok=True)
    total_rows = sum(len(c) for c in chunks)

    if len(chunks) == 1:
        chunks[0].to_csv(os.path.join(month_dir, f"processo_{process_id}.csv"), index=False)
    else:
        for i, chunk in enumerate(chunks, start=1):
            chunk.to_csv(os.path.join(month_dir, f"processo_{process_id}-parte{i}.csv"), index=False)

    return total_rows, len(chunks)


def already_exported(process_id: str, month_label: str) -> bool:
    """True se já existe CSV desse (processo, mês) em dump ou made — pra retomar sem refazer."""
    for base in (DUMP_DIR, MADE_DIR):
        d = os.path.join(base, month_label)
        if glob.glob(os.path.join(d, f"processo_{process_id}.csv")) or \
           glob.glob(os.path.join(d, f"processo_{process_id}-parte*.csv")):
            return True
    return False


def main():
    print(f"\n{'='*57}")
    print(f"  Cluster       : {CLUSTER_URI}")
    print(f"  Database      : {DATABASE}")
    print(f"  Janela        : últimos {DAYS_BACK} dias")
    print(f"  Ajuste de hora: ADX (UTC) + {HOURS_OFFSET}h  (+{ALT_HOURS_OFFSET}h em {len(ALT_PROCESSES)} processos de Manaus)")
    print(f"{'='*57}\n")

    client = build_client()

    print("Buscando IDs de processos no ADX...")
    process_ids = fetch_process_ids(client)

    months = get_months_range()
    total = len(process_ids)

    print(f"Total de processos : {total}")
    print(f"Meses a processar  : {[m[2] for m in months]}\n")

    skipped = 0
    errors = 0

    for idx, process_id in enumerate(process_ids, start=1):
        manaus = "  [Manaus +4h]" if offset_for(process_id) == ALT_HOURS_OFFSET else ""
        print(f"[{idx}/{total}] {process_id}{manaus}")

        # retomável: só os meses cujo CSV ainda não existe (em dump ou made)
        pending_months = [m for m in months if not already_exported(process_id, m[2])]
        if not pending_months:
            print(f"        já exportado (todos os meses) — pulando")
            skipped += 1
            continue

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

        print(f"        {total_rows:,} linhas no total — extraindo {len(pending_months)} mês(es) pendente(s)...")

        for start, end, label in pending_months:
            try:
                chunks = fetch_chunks(client, process_id, start, end)

                if not chunks:
                    print(f"        [{label}] sem dados, pulando")
                    continue

                month_dir = os.path.join(DUMP_DIR, label)
                saved_rows, n_parts = save_chunks(chunks, month_dir, process_id)

                if n_parts == 1:
                    print(f"        [{label}] OK  ({saved_rows:,} linhas)  →  {month_dir}/processo_{process_id}.csv")
                else:
                    print(f"        [{label}] OK  ({saved_rows:,} linhas em {n_parts} partes)  →  {month_dir}/processo_{process_id}-parteN.csv")

            except Exception as exc:
                print(f"        [{label}] ERRO [{type(exc).__name__}]: {exc}")
                errors += 1

    print(f"\n{'='*57}")
    print(f"  Extração concluída.")
    print(f"  Ignorados (0 linhas) : {skipped}")
    print(f"  Erros                : {errors}")
    print(f"  Saída                : {DUMP_DIR}/")
    print(f"{'='*57}\n")


if __name__ == "__main__":
    main()
