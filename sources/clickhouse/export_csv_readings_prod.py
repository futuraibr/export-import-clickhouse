"""
Filtros passados na linha de comando (TODOS obrigatórios):
  --company-id   empresa a puxar
  --start        início do período (formato "YYYY-MM-DD HH:MM:SS", com aspas)
  --end          fim do período    (formato "YYYY-MM-DD HH:MM:SS", com aspas)
  --no-final     (opcional) desliga o FINAL na query (mais rápido, pode trazer duplicata)
"""

import argparse
import os
import sys
import time
from datetime import datetime

# acha a raiz do projeto pra conseguir importar o common/
_root = os.path.dirname(os.path.abspath(__file__))
while _root != os.path.dirname(_root) and not os.path.isdir(os.path.join(_root, "common")):
    _root = os.path.dirname(_root)
sys.path.insert(0, _root)

import pandas as pd

from common import config
from common.clickhouse_prod import get_clickhouse_client
from common.progress import SUMMARY_MARKER, upsert_entry

CH_DATABASE    = config.CH_DATABASE_PROD
READINGS_TABLE = "readings"
BATCH_ROWS     = config.BATCH_ROWS_PROD

# pastas separadas por ambiente — prod usa a subpasta prod (made/prod, output/prod)
_BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
MADE_DIR      = os.path.join(_BASE_DIR, "made", "prod")
OUTPUT_DIR    = os.path.join(_BASE_DIR, "output", "prod")
RECEBIDOS_LOG = os.path.join(OUTPUT_DIR, ".recebidos.log")

DT_FORMAT = "%Y-%m-%d %H:%M:%S"

# formato do TIMESTAMP na CSV (igual ao exemplo: sem segundos)
TS_OUT_FORMAT = "%Y-%m-%d %H:%M"


def parse_dt(value: str) -> datetime:
    """Valida o formato "YYYY-MM-DD HH:MM:SS"; erro claro se não bater."""
    try:
        return datetime.strptime(value, DT_FORMAT)
    except ValueError:
        raise SystemExit(f"ERRO: data/hora inválida: '{value}'. Use o formato \"YYYY-MM-DD HH:MM:SS\".")


def _slug(dt_str: str) -> str:
    """Deixa a data/hora amigável pra nome de arquivo: mantém a data, tira ':' e troca espaço por '_'."""
    return dt_str.replace(":", "").replace(" ", "_")


def load_recebidos(log_path: str) -> list[dict]:
    """Lê o log atual pra não perder o histórico dos exports anteriores (só file + linhas)."""
    if not os.path.exists(log_path):
        return []
    entries, cur = [], None
    with open(log_path) as f:
        for line in f:
            line = line.rstrip()
            if line.startswith(SUMMARY_MARKER):
                break
            if line.startswith("arquivo:"):
                if cur:
                    entries.append(cur)
                cur = {"file": line.split(":", 1)[1].strip(), "company": "", "period": "", "rows": 0, "elapsed": ""}
            elif cur is None:
                continue
            elif "Empresa" in line:
                cur["company"] = line.split(":", 1)[1].strip()
            elif "Período" in line:
                cur["period"] = line.split(":", 1)[1].strip()
            elif "Concluído em" in line:
                cur["elapsed"] = line.split(":", 1)[1].strip()
            elif "Linhas exportadas" in line:
                cur["rows"] = int(line.split(":", 1)[1].strip().replace(",", "").replace(".", "") or 0)
    if cur:
        entries.append(cur)
    return entries


def write_recebidos_log(log_path: str, entries: list[dict]):
    """Regrava o .recebidos.log com a lista de exports e um total no fim (mesma pegada do .enviados.log)."""
    os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
    total_rows = sum(e["rows"] for e in entries)

    with open(log_path, "w") as f:
        for e in entries:
            f.write(f"arquivo: {e['file']}\n\n")
            f.write(f"  Concluído em     : {e['elapsed']}\n")
            f.write(f"  Empresa          : {e['company']}\n")
            f.write(f"  Período          : {e['period']}\n")
            f.write(f"  Linhas exportadas: {e['rows']:,}\n")
            f.write("\n")
        f.write(f"{SUMMARY_MARKER}\n")
        f.write("RESUMO TOTAL\n\n")
        f.write(f"  Arquivos ({len(entries)}):\n")
        for e in entries:
            f.write(f"    - {e['file']}\n")
        f.write("\n")
        f.write(f"  Linhas exportadas: {total_rows:,}\n")
        f.write(f"{SUMMARY_MARKER}\n")


def export_to_csv(client, company_id: str, start: str, end: str, use_final: bool, out_path: str) -> int:
    """Puxa a fatia da readings e grava em CSV no formato WIDE (pivot):
    1ª coluna TIMESTAMP (um período por linha), demais colunas = cada tag_id com seus valores.
    Devolve o número de linhas (períodos) da CSV."""
    final = "FINAL" if use_final else ""
    query = f"""
        SELECT ts, tag_id, value
        FROM {CH_DATABASE}.{READINGS_TABLE} {final}
        WHERE company_id = {{company_id:String}}
          AND ts >= {{start:String}}
          AND ts <= {{end:String}}
        ORDER BY ts, tag_id
    """
    params = {"company_id": company_id, "start": start, "end": end}

    frames = []
    datapoints = 0
    with client.query_df_stream(query, parameters=params) as stream:
        for block in stream:
            if block.empty:
                continue
            frames.append(block)
            datapoints += len(block)
            print(f"    +{len(block):,} pontos (acumulado {datapoints:,})", flush=True)

    if datapoints == 0:
        return 0

    long_df = pd.concat(frames, ignore_index=True)
    # pivot long → wide: uma linha por ts, uma coluna por tag_id
    wide = long_df.pivot_table(index="ts", columns="tag_id", values="value", aggfunc="first")
    wide = wide.sort_index()
    wide.columns.name = None
    # TIMESTAMP como 1ª coluna (formato igual ao exemplo, sem segundos)
    wide.insert(0, "TIMESTAMP", wide.index.strftime(TS_OUT_FORMAT))

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    # separador ';' + BOM (utf-8-sig) pra abrir certinho no Excel PT-BR, igual ao exemplo
    wide.to_csv(out_path, index=False, sep=";", encoding="utf-8-sig")

    return len(wide)


def main():
    parser = argparse.ArgumentParser(
        description="Exporta a tabela readings (ClickHouse PROD) para CSV, filtrando por empresa e período."
    )
    parser.add_argument("--company-id", required=True, help="company_id da empresa a exportar (obrigatório).")
    parser.add_argument("--start", required=True, help='Início do período "YYYY-MM-DD HH:MM:SS" (obrigatório).')
    parser.add_argument("--end", required=True, help='Fim do período "YYYY-MM-DD HH:MM:SS" (obrigatório).')
    parser.add_argument("--no-final", action="store_true", help="Desliga o FINAL na query (mais rápido, pode duplicar).")
    args = parser.parse_args()

    # valida datas antes de qualquer conexão
    dt_start = parse_dt(args.start)
    dt_end = parse_dt(args.end)
    if dt_start > dt_end:
        raise SystemExit(f"ERRO: --start ({args.start}) é depois de --end ({args.end}).")

    company_id = args.company_id.strip()
    use_final = not args.no_final
    period_label = f"{args.start} → {args.end}"

    out_name = f"{company_id}_{_slug(args.start)}_a_{_slug(args.end)}.csv"
    out_path = os.path.join(MADE_DIR, out_name)

    print(f"\n{'='*57}")
    print(f"  Host       : {config.CH_HOST_PROD}:{config.CH_PORT_PROD}")
    print(f"  Tabela     : {CH_DATABASE}.{READINGS_TABLE}{' FINAL' if use_final else ''}")
    print(f"  Empresa    : {company_id}")
    print(f"  Período    : {period_label}  (ts do banco, sem conversão)")
    print(f"  Saída      : {out_path}")
    print(f"{'='*57}\n")

    client = get_clickhouse_client()
    print("Conexão com ClickHouse OK\n")

    t0 = time.time()
    total = export_to_csv(client, company_id, args.start, args.end, use_final, out_path)
    elapsed = time.time() - t0

    if total == 0:
        # não deixa CSV vazio nem registra no log
        if os.path.exists(out_path):
            os.remove(out_path)
        print(f"\nNenhuma linha no período para essa empresa — nada exportado.")
        print(f"{'='*57}\n")
        return

    entries = load_recebidos(RECEBIDOS_LOG)
    upsert_entry(entries, {
        "file": out_name,
        "company": company_id,
        "period": period_label,
        "rows": total,
        "elapsed": f"{elapsed:.1f}s",
    })
    write_recebidos_log(RECEBIDOS_LOG, entries)

    print(f"\n{'='*57}")
    print(f"  Concluído em     : {elapsed:.1f}s")
    print(f"  Linhas exportadas: {total:,}")
    print(f"  Arquivo          : {out_path}")
    print(f"  Log de recebidos : {RECEBIDOS_LOG}")
    print(f"{'='*57}\n")


if __name__ == "__main__":
    main()
