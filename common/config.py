"""Configurações do projeto, lidas do .env (com padrões pra rodar sem ele)."""

import os
from dotenv import load_dotenv

# pega o .env da raiz do projeto, não importa de onde o script for rodado
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_REPO_ROOT, ".env"))


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw not in (None, "") else default


# ClickHouse DEV — valores reais ficam no .env; aqui só fallback (segredo em branco)
CH_HOST     = os.environ.get("CH_HOST", "")
CH_PORT     = _int("CH_PORT", 8123)
CH_DATABASE = os.environ.get("CH_DATABASE", "futurai_db")
CH_USER     = os.environ.get("CH_USER", "")
CH_PASSWORD = os.environ.get("CH_PASSWORD", "")
CH_SECURE   = os.environ.get("CH_SECURE", "false").lower() in ("1", "true", "yes")
CH_TABLE    = os.environ.get("CH_TABLE", "readings")
CH_LOG      = os.environ.get("CH_LOG", "operation_log")

# ClickHouse PROD — preencher as chaves CH_*_PROD no .env (usado pelos scripts _prod)
CH_HOST_PROD     = os.environ.get("CH_HOST_PROD", "")
CH_PORT_PROD     = _int("CH_PORT_PROD", 8123)
CH_DATABASE_PROD = os.environ.get("CH_DATABASE_PROD", "futurai_db")
CH_USER_PROD     = os.environ.get("CH_USER_PROD", "")
CH_PASSWORD_PROD = os.environ.get("CH_PASSWORD_PROD", "")
CH_SECURE_PROD   = os.environ.get("CH_SECURE_PROD", "false").lower() in ("1", "true", "yes")
CH_TABLE_PROD    = os.environ.get("CH_TABLE_PROD", "readings")
CH_LOG_PROD      = os.environ.get("CH_LOG_PROD", "operation_log")

# Azure Data Explorer DEV (origem do fluxo ADX)
ADX_CLUSTER_URI = os.environ.get("ADX_CLUSTER_URI", "https://futurai.brazilsouth.kusto.windows.net")
ADX_DATABASE    = os.environ.get("ADX_DATABASE", "variables_dev")
ADX_TABLE       = os.environ.get("ADX_TABLE", "variables_a57d9b153ff144d9a2b6e7e8e3a04dc3")

# Azure Data Explorer PROD (origem do export_adx_prod) — preencher no .env
ADX_CLUSTER_URI_PROD = os.environ.get("ADX_CLUSTER_URI_PROD", "")   # ADICIONAR XXX AQUI (cluster de prod, se for diferente)
ADX_DATABASE_PROD    = os.environ.get("ADX_DATABASE_PROD", "")      # ADICIONAR XXX AQUI (database de prod, ex.: variables_prod)
ADX_TABLE_PROD       = os.environ.get("ADX_TABLE_PROD", "")         # ADICIONAR XXX AQUI (tabela de prod)

# valores de negócio DEV
COMPANY_ID     = os.environ.get("COMPANY_ID", "a57d9b153ff144d9a2b6e7e8e3a04dc3")
MAX_PAST_HOURS = _int("MAX_PAST_HOURS", 4320)   # mesmo limite que o Lambda usa
BATCH_ROWS     = _int("BATCH_ROWS", 50_000)
DAYS_BACK      = _int("DAYS_BACK", 180)
CHUNK_LIMIT    = _int("CHUNK_LIMIT", 400_000)   # fica abaixo do teto de 500k do ADX

# valores de negócio PROD — usados pelos scripts _prod. Default = mesmo do dev por enquanto;
# ajustar no .env quando for a migração real (ex.: janela maior).
COMPANY_ID_PROD     = os.environ.get("COMPANY_ID_PROD") or COMPANY_ID   # em branco = mesma empresa do dev
MAX_PAST_HOURS_PROD = _int("MAX_PAST_HOURS_PROD", MAX_PAST_HOURS)
BATCH_ROWS_PROD     = _int("BATCH_ROWS_PROD", BATCH_ROWS)
DAYS_BACK_PROD      = _int("DAYS_BACK_PROD", DAYS_BACK)
CHUNK_LIMIT_PROD    = _int("CHUNK_LIMIT_PROD", CHUNK_LIMIT)

# tabelas do Dynamo (dump/made/output ficam dentro da pasta de cada script)
PREDICTIONS_TABLE = os.environ.get("PREDICTIONS_TABLE", "predictions")
PRED_BATCH_ROWS   = _int("PRED_BATCH_ROWS", 10_000)

PROJECTIONS_TABLE = os.environ.get("PROJECTIONS_TABLE", "projections")
PROJ_BATCH_ROWS   = _int("PROJ_BATCH_ROWS", 10_000)

# Model config / unidade (acesso ao vivo no DynamoDB; usa o COMPANY_ID acima)
# SEMPRE DEV: pinar AWS_PROFILE no .env pro profile de dev evita esbarrar em produção.
AWS_PROFILE       = os.environ.get("AWS_PROFILE", "")                  # vazio = credencial ambiente
AWS_REGION        = os.environ.get("AWS_REGION", "us-east-1")          # CONFIRMAR região das tabelas (dev)
MODEL_CONFIG_TYPE = os.environ.get("MODEL_CONFIG_TYPE", "PREDICTION_TRIGGER")
