# Migration

Scripts de migração de dados para o **ClickHouse** (`futurai_db`). Os fluxos ficam em
[`sources/`](sources/), organizados por **sistema de origem**; os utilitários compartilhados
ficam em [`common/`](common/).

| Fluxo | Origem | Destino (tabela) | Pasta |
|-------|--------|------------------|-------|
| **ADX** | Azure Data Explorer (Kusto) | `readings` (+ `operation_log`) | [`sources/adx/`](sources/adx/) |
| **Predictions** | Export do DynamoDB (`.json.gz`) | `predictions` | [`sources/dynamo/predictions/`](sources/dynamo/predictions/) |
| **Projections** | Export do DynamoDB (`.json.gz`) | `projections` | [`sources/dynamo/projections/`](sources/dynamo/projections/) |
| **Model config / unidade** | DynamoDB ao vivo (`model_config_*` + `process_*`) | DynamoDB `model_config_*` (campo `unit_id`) | [`sources/dynamo/model_config/`](sources/dynamo/model_config/) |

---

## 1. Setup (uma vez)

```bash
# 1. Ambiente virtual
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

# 2. Dependências
pip install -r requirements.txt

# 3. Configuração: copie o exemplo e ajuste as credenciais
cp .env.example .env
# edite o .env (CH_HOST, CH_PASSWORD, etc.)
```

> O `.env` (na raiz) centraliza todas as credenciais e constantes — ClickHouse, ADX e
> regras de negócio. Os scripts leem dele via [`common/config.py`](common/config.py).
> Os defaults batem com o ambiente atual, então sem `.env` eles ainda funcionam.
> Os scripts podem ser rodados de qualquer pasta (acham `common/` automaticamente).

**Padrão das pastas de dados** (igual em todos os fluxos):

```
<fluxo>/
├── dump/     ← arquivos a processar (entrada)
├── made/     ← já processados (o script MOVE para cá após inserir)
└── output/   ← .enviados.log (relatório do que foi enviado, acumula entre execuções)
```

Reexecutar é seguro: só o que está em `dump/` é processado; o que já foi para `made/`
é ignorado. As pastas `dump/`, `made/` e `output/` não vão para o git.

---

## 2. ADX → ClickHouse

Duas etapas: **exportar** do ADX para CSV e depois **importar** os CSVs no ClickHouse.

### 2.1. Exportar (ADX → `sources/adx/dump/<mês>/`)

```bash
python sources/adx/export_adx.py
# opcional: timezone de origem dos timestamps (default America/Sao_Paulo)
python sources/adx/export_adx.py --timezone UTC
```

- Usa autenticação interativa (device code) do Azure — abre um código no terminal.
- Busca os últimos `DAYS_BACK` dias (default 180), fatiados por mês.
- Lida com o teto de 500k linhas do ADX dividindo o intervalo recursivamente.
- Grava `sources/adx/dump/<mês>/processo_<id>.csv` (ou `-parteN.csv` quando o mês é grande).

### 2.2. Importar em lote (`sources/adx/dump/` → ClickHouse `readings`)

```bash
python sources/adx/import_clickhouse.py
```

- Lê todos os CSVs de `dump/`, aplica as regras de validação do Lambda
  (`bad_value`, `timestamp_future`, `timestamp_too_old`) e insere em `readings`.
- **Move** cada CSV importado para `made/<mês>/` e registra em `output/.enviados.log`.
- Se um arquivo falhar, ele **permanece em `dump/`** para a próxima execução.

### 2.3. Importar um processo específico + auditoria (opcional)

```bash
python sources/adx/import_clickhouse_single.py <process_id>
python sources/adx/import_clickhouse_single.py <process_id> --logs-only   # só grava operation_log
```

- Ferramenta pontual: processa só os CSVs de um `process_id` (procura em `dump/` e `made/`).
- Além de `readings`, grava os descartes em `operation_log` (consolidados por tag).
- **Não move** arquivos; tem log próprio em `output/.enviados_single.log`.

---

## 3. Predictions → ClickHouse

Os dados já vêm exportados do DynamoDB como `.json.gz` (formato tipado da AWS).
Não há conexão com o DynamoDB — só leitura local dos arquivos.

### 3.1. Onde colocar os arquivos

Coloque os `.json.gz` em **`sources/dynamo/predictions/dump/`** (pode ser a pasta inteira
do export, com `data/` e `manifest-*` — os manifests são ignorados).

### 3.2. Validar antes de carregar (recomendado)

```bash
# Mostra uma amostra transformada e estatísticas, SEM inserir nada:
python sources/dynamo/predictions/import_predictions_json.py --dry-run --file <arquivo>.json.gz --limit 20
```

> A tabela `predictions` já deve existir no ClickHouse (criada fora deste projeto).
> O script só insere — não cria nem apaga tabela.

### 3.3. Carga real

```bash
python sources/dynamo/predictions/import_predictions_json.py
```

- Lê os `.json.gz` de `dump/`, achata o JSON do DynamoDB e insere em lotes.
- Colunas migradas: `company_id, process_id, ts (← timestamp), index, threshold`
  (a coluna `model` do JSON é ignorada).
- **Move** cada arquivo para `made/`, registra em `output/.enviados.log`
  e roda `OPTIMIZE TABLE ... FINAL` no fim (consolida o ReplacingMergeTree).
- Flags úteis: `--limit N`, `--no-optimize`.

---

## 4. Projections → ClickHouse

Mesmo padrão do Predictions: os dados vêm do export do DynamoDB (`projections_<company_id>`)
como `.json.gz`. O destino `projections` é **formato LONG** (`company_id, process_id, tag_id,
value, ts`), enquanto o DynamoDB é **WIDE** (1 item por timestamp com várias colunas de tag) —
o script faz o **unpivot wide→long** automaticamente (e também aceita itens já no formato long).

### 4.1. Onde colocar os arquivos
Coloque os `.json.gz` em **`sources/dynamo/projections/dump/`**.

### 4.2. Validar antes de carregar (recomendado)
```bash
python sources/dynamo/projections/import_projections_json.py --dry-run --file <arquivo>.json.gz --limit 20
```

> A tabela `projections` já deve existir no ClickHouse (criada fora deste projeto).
> O script só insere — não cria nem apaga tabela.

### 4.3. Carga real
```bash
python sources/dynamo/projections/import_projections_json.py
```
Lê os `.json.gz` de `dump/`, faz o unpivot, insere em lotes, **move** para `made/`,
registra em `output/.enviados.log` e roda `OPTIMIZE TABLE ... FINAL` no fim.
Flags: `--limit N`, `--no-optimize`.

---

## 5. Model config → `unit_id` (unidade) — DynamoDB ao vivo

Diferente dos demais, este fluxo **não** mexe no ClickHouse nem lê arquivos: ele acessa o
DynamoDB ao vivo (via `boto3`) e **grava de volta** no próprio DynamoDB.

**Regra:** cada `model_config` ativo está amarrado a um `process_id`. Subindo a hierarquia de
processos (campo `level`, que aponta pro pai; no topo vale `"root"`) até o nó cujo `level == "root"`,
o `process_id` desse nó é a **unidade**. Esse valor é gravado como `unit_id` no item do model_config.

1. Scan em `model_config_<COMPANY_ID>` filtrando `active == True` **e** `type == <MODEL_CONFIG_TYPE>`.
2. Pra cada item, `get` em `process_<COMPANY_ID>` lendo o `level` e subindo recursivamente até `root`.
3. Grava `unit_id` (= process_id do nó raiz) no item via `update_item` (chave `process_id` + `created_at`).
4. Proteção contra loop infinito: set de visitados (detecta ciclo), teto de profundidade e checagem de
   processo inexistente.

**Pré-requisitos:** `AWS_REGION` (região das tabelas — **confirme antes de aplicar**) e credenciais AWS
na cadeia padrão do `boto3` (env / `~/.aws` / profile). Veja `MODEL_CONFIG_TYPE` no `.env`.

### 5.1. Conferir antes (DRY-RUN — não escreve)
```bash
python sources/dynamo/model_config/backfill_unit_id.py
```
Mostra, pra cada model_config ativo, qual `unit_id` seria gravado, e um resumo (a atualizar / já
corretos / erros). Nenhuma escrita acontece sem `--apply`.

### 5.2. Aplicar (escreve no DynamoDB)
```bash
python sources/dynamo/model_config/backfill_unit_id.py --apply
# opcional: outra empresa
python sources/dynamo/model_config/backfill_unit_id.py --apply --company-id <id>
```
Idempotente: itens cujo `unit_id` já bate são pulados.

### 5.3. Rodar na Lambda "coringa" da AWS
O arquivo [`lambda_coringa_snippet.py`](sources/dynamo/model_config/lambda_coringa_snippet.py) é a
mesma regra, **autocontida** (importa `boto3` direto, região herdada do ambiente da Lambda). Não é
executado por este repo — é pra **copiar e colar** na função coringa do console AWS. Lá, teste
primeiro com `{"apply": false}` no event antes de rodar com `apply=True`.

---

## Estrutura do repositório

```
.
├── common/                       utilitários compartilhados
│   ├── config.py                 credenciais/constantes (lê do .env)
│   ├── clickhouse.py             get_clickhouse_client() + insert em lote
│   ├── dynamo.py                 get_dynamodb_resource() (boto3, região do .env)
│   ├── validation.py             regras de ingestão do Lambda (readings)
│   └── progress.py               mover dump→made + .enviados.log
├── sources/
│   ├── adx/                      export ADX + import (readings)
│   └── dynamo/
│       ├── predictions/          import .json.gz (predictions)
│       ├── projections/          import .json.gz (projections, wide→long)
│       └── model_config/         resolve unit_id (raiz) e grava no DynamoDB
├── requirements.txt
├── .env.example
└── README.md
```
