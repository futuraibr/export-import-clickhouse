"""Configurações do projeto, lidas do .env (com padrões pra rodar sem ele)."""

import os
from dotenv import load_dotenv

# pega o .env da raiz do projeto, não importa de onde o script for rodado
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_REPO_ROOT, ".env"))


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw not in (None, "") else default


# ClickHouse PROD — preencher as chaves CH_*_PROD no .env
CH_HOST_PROD     = os.environ.get("CH_HOST_PROD", "")
CH_PORT_PROD     = _int("CH_PORT_PROD", 8123)
CH_DATABASE_PROD = os.environ.get("CH_DATABASE_PROD", "futurai_db")
CH_USER_PROD     = os.environ.get("CH_USER_PROD", "")
CH_PASSWORD_PROD = os.environ.get("CH_PASSWORD_PROD", "")
CH_SECURE_PROD   = os.environ.get("CH_SECURE_PROD", "false").lower() in ("1", "true", "yes")
CH_TABLE_PROD    = os.environ.get("CH_TABLE_PROD", "readings")

# Azure Data Explorer PROD (origem do export_adx_prod) — preencher no .env
ADX_CLUSTER_URI_PROD = os.environ.get("ADX_CLUSTER_URI_PROD", "")
ADX_DATABASE_PROD    = os.environ.get("ADX_DATABASE_PROD", "")
ADX_TABLE_PROD       = os.environ.get("ADX_TABLE_PROD", "")

# valores de negócio (defaults; ajustar no .env)
COMPANY_ID     = os.environ.get("COMPANY_ID", "a57d9b153ff144d9a2b6e7e8e3a04dc3")
MAX_PAST_HOURS = _int("MAX_PAST_HOURS", 4320)   # mesmo limite que o Lambda usa
BATCH_ROWS     = _int("BATCH_ROWS", 50_000)

COMPANY_ID_PROD     = os.environ.get("COMPANY_ID_PROD") or COMPANY_ID   # em branco = mesma empresa
MAX_PAST_HOURS_PROD = _int("MAX_PAST_HOURS_PROD", MAX_PAST_HOURS)
BATCH_ROWS_PROD     = _int("BATCH_ROWS_PROD", BATCH_ROWS)
DAYS_BACK_PROD      = _int("DAYS_BACK_PROD", 180)

# tabelas do Dynamo (dump/made/output ficam dentro da pasta de cada script)
PREDICTIONS_TABLE = os.environ.get("PREDICTIONS_TABLE", "predictions")
PRED_BATCH_ROWS   = _int("PRED_BATCH_ROWS", 10_000)
PROJECTIONS_TABLE = os.environ.get("PROJECTIONS_TABLE", "projections")
PROJ_BATCH_ROWS   = _int("PROJ_BATCH_ROWS", 10_000)

# Model config / unidade (acesso ao vivo no DynamoDB; usa o COMPANY_ID acima)
# SEMPRE DEV: pinar AWS_PROFILE no .env pro profile de dev evita esbarrar em produção.
AWS_PROFILE       = os.environ.get("AWS_PROFILE", "")                  # vazio = credencial ambiente
AWS_REGION        = os.environ.get("AWS_REGION", "us-east-1")          # CONFIRMAR região das tabelas
MODEL_CONFIG_TYPE = os.environ.get("MODEL_CONFIG_TYPE", "PREDICTION_TRIGGER")
