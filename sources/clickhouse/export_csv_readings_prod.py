import os
import sys
import time
from datetime import datetime, timezone

# acha a raiz do projeto pra conseguir importar o common/
_root = os.path.dirname(os.path.abspath(__file__))
while _root != os.path.dirname(_root) and not os.path.isdir(os.path.join(_root, "common")):
    _root = os.path.dirname(_root)
sys.path.insert(0, _root)

import clickhouse_connect
import pandas as pd

from common import config
from common.progress import SUMMARY_MARKER, upsert_entry

READINGS_TABLE = "readings"
TS_OUT_FORMAT  = "%Y-%m-%d %H:%M"   # formato do TIMESTAMP na CSV (sem segundos)
DB_DT_FORMAT   = "%Y-%m-%d %H:%M:%S"

# formatos aceitos nas datas do YAML (com ou sem segundos; '-' ou espaço)
_DT_INPUT_FORMATS = ("%Y-%m-%d-%H:%M:%S", "%Y-%m-%d-%H:%M",
                     "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M")

# pastas separadas por ambiente — prod usa a subpasta prod (made/prod, output/prod)
_BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
MADE_DIR      = os.path.join(_BASE_DIR, "made", "prod")
OUTPUT_DIR    = os.path.join(_BASE_DIR, "output", "prod")
RECEBIDOS_LOG = os.path.join(OUTPUT_DIR, ".recebidos.log")


def _to_bool(value, default=False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "sim")


def _strip_inline_comment(s: str) -> str:
    """Remove comentário inline (' # ...'), preservando '#' colado no valor (ex.: tag CA-H1_#170)."""
    for i, ch in enumerate(s):
        if ch == "#" and (i == 0 or s[i - 1] in " \t"):
            return s[:i].rstrip()
    return s


def load_yaml_config(path: str):
    """Parser simples do YAML (sem depender de PyYAML): aceita 'CHAVE: valor'
    (ou 'CHAVE:valor') e uma lista TAGS com itens no formato '- tag'."""
    if not os.path.exists(path):
        raise SystemExit(f"ERRO: arquivo de configuração não encontrado: {path}")
    cfg, tags, in_tags = {}, [], False
    with open(path, encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("-"):                      # item de lista (tag)
                if in_tags:
                    tag = _strip_inline_comment(line[1:].strip()).strip().strip('"').strip("'")
                    if tag:
                        tags.append(tag)
                continue
            key, sep, val = line.partition(":")           # 'CHAVE: valor'
            key = key.strip().upper()
            val = _strip_inline_comment(val.strip()).strip().strip('"').strip("'")
            if key == "TAGS":
                in_tags = True
                continue
            in_tags = False
            if sep:
                cfg[key] = val
    return cfg, tags


def parse_cfg_dt(value: str, field: str) -> datetime:
    """Lê a data do YAML (YYYY-MM-DD-HH:MM, segundos opcionais → padrão :00)."""
    v = str(value).strip()
    for fmt in _DT_INPUT_FORMATS:
        try:
            return datetime.strptime(v, fmt)
        except ValueError:
            continue
    raise SystemExit(
        f"ERRO: {field} inválido: '{value}'. Use 'YYYY-MM-DD-HH:MM' (segundos opcionais, ex.: 2026-06-25-11:00)."
    )


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


def export_to_csv(client, database, company_id, start, end, tags, use_final, out_path) -> int:
    """Puxa a fatia da readings e grava em CSV no formato WIDE (pivot):
    1ª coluna TIMESTAMP (um período por linha), demais colunas = cada tag_id com seus valores.
    Devolve o número de linhas (períodos) da CSV."""
    final = "FINAL" if use_final else ""
    params = {"company_id": company_id, "start": start, "end": end}
    tag_clause = ""
    if tags:
        tag_clause = "AND tag_id IN {tags:Array(String)}"
        params["tags"] = tags

    query = f"""
        SELECT ts, tag_id, value
        FROM {database}.{READINGS_TABLE} {final}
        WHERE company_id = {{company_id:String}}
          AND ts >= {{start:String}}
          AND ts <= {{end:String}}
          {tag_clause}
        ORDER BY ts, tag_id
    """

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
    # TIMESTAMP como 1ª coluna (sem segundos)
    wide.insert(0, "TIMESTAMP", wide.index.strftime(TS_OUT_FORMAT))

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    # separador ';' + BOM (utf-8-sig) pra abrir certinho no Excel PT-BR
    wide.to_csv(out_path, index=False, sep=";", encoding="utf-8-sig")
    return len(wide)


def main():
    config_path = sys.argv[1] if len(sys.argv) > 1 else "export_config.yaml"
    cfg, tags = load_yaml_config(config_path)

    # ---- conexão: do YAML se vier; senão do .env (common/config) ----
    host     = cfg.get("HOST")     or config.CH_HOST_PROD
    port     = int(cfg.get("PORT") or config.CH_PORT_PROD)
    database = cfg.get("DATABASE") or config.CH_DATABASE_PROD
    user     = cfg.get("USER")     or config.CH_USER_PROD
    password = cfg.get("PASSWORD") or config.CH_PASSWORD_PROD
    secure   = _to_bool(cfg.get("SECURE"), True) if "SECURE" in cfg else config.CH_SECURE_PROD

    # ---- filtros (obrigatórios) ----
    company_id = (cfg.get("COMPANY_ID") or "").strip()
    if not company_id:
        raise SystemExit("ERRO: COMPANY_ID é obrigatório no YAML.")
    if "DATE_INI" not in cfg or "DATE_END" not in cfg:
        raise SystemExit("ERRO: DATE_INI e DATE_END são obrigatórios no YAML.")

    dt_ini = parse_cfg_dt(cfg["DATE_INI"], "DATE_INI")
    dt_end = parse_cfg_dt(cfg["DATE_END"], "DATE_END")
    if dt_ini > dt_end:
        raise SystemExit(f"ERRO: DATE_INI ({cfg['DATE_INI']}) é depois de DATE_END ({cfg['DATE_END']}).")
    # o ts no banco está em UTC; não há dado no futuro
    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    if dt_ini > now_utc:
        raise SystemExit(
            f"ERRO: DATE_INI ({cfg['DATE_INI']}) está no futuro (agora: {now_utc:%Y-%m-%d %H:%M} UTC) — não há dados a partir daí."
        )
    if dt_end > now_utc:                                   # fim no futuro: corta até agora e avisa
        print(f"AVISO: DATE_END ({cfg['DATE_END']}) está no futuro; ajustando para agora ({now_utc:%Y-%m-%d %H:%M} UTC).\n")
        dt_end = now_utc
    start = dt_ini.strftime(DB_DT_FORMAT)
    end   = dt_end.strftime(DB_DT_FORMAT)

    use_final    = not _to_bool(cfg.get("NO_FINAL"), False)
    period_label = f"{start} → {end}"
    out_name = f"{company_id}_{dt_ini.strftime('%Y-%m-%d_%H%M%S')}_a_{dt_end.strftime('%Y-%m-%d_%H%M%S')}.csv"
    out_path = os.path.join(MADE_DIR, out_name)

    print(f"\n{'='*57}")
    print(f"  Config     : {config_path}")
    print(f"  Host       : {host}:{port}")
    print(f"  Tabela     : {database}.{READINGS_TABLE}{' FINAL' if use_final else ''}")
    print(f"  Empresa    : {company_id}")
    print(f"  Período    : {period_label}  (ts do banco, sem conversão)")
    print(f"  Tags       : {len(tags) if tags else 'TODAS'}")
    print(f"  Saída      : {out_path}")
    print(f"{'='*57}\n")

    client = clickhouse_connect.get_client(
        host=host, port=port, username=user, password=password, database=database, secure=secure,
    )
    print("Conexão com ClickHouse OK\n")

    t0 = time.time()
    total = export_to_csv(client, database, company_id, start, end, tags, use_final, out_path)
    elapsed = time.time() - t0

    if total == 0:
        if os.path.exists(out_path):
            os.remove(out_path)
        print("\nNenhuma linha no período/tags para essa empresa — nada exportado.")
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
    print(f"  Linhas (períodos): {total:,}")
    print(f"  Arquivo          : {out_path}")
    print(f"  Log de recebidos : {RECEBIDOS_LOG}")
    print(f"{'='*57}\n")


if __name__ == "__main__":
    main()
