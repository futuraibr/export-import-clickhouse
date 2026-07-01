# Comandos de uso

Referência rápida e direta de cada importação. Rode tudo a partir da **raiz do projeto** (`migration/`), com o venv ativo:

```bash
source .venv/bin/activate
```
(ou troque `python` por `.venv/bin/python` nos comandos abaixo)

**Fuso (todos os fluxos):** o `ts` é gravado com **+3h** por padrão e **+4h** nos 6 processos de Manaus
(`668dfaa49d19463e9fb0082f8ae52c2f`, `2e7fa9dccd574a1494a95b43ca76d5f2`, `567b3498d5d9469681fe1586591fbd9e`,
`f0c7130974924b70a34b7b9516b1036c`, `b2fd0a766acd40819d8e34df4ba81c48`, `ef41b354e25948b49667f948694a0957`).

---

## 1) readings (ADX → tabela `readings`)

Dois passos: **exportar** do ADX (gera CSV) e **importar** no ClickHouse.

**Exportar** (puxa do ADX, pede login, gera CSVs em `sources/adx/dump/prod/`):
```bash
ADX_TABLE_PROD=variables_{company_id} python sources/adx/export_adx_prod.py
```

**Importar** (lê `sources/adx/dump/prod/`, insere em `readings`, move pra `made/prod/`, roda OPTIMIZE por partição):
```bash
COMPANY_ID_PROD={company_id} python sources/adx/import_clickhouse_prod.py
```
> O CSV não tem company_id → **`COMPANY_ID_PROD` é obrigatório** no import. Uma empresa por vez.
> O export é retomável e tem retry de throttling (pode rodar de novo se cair).

---

## 2) projections (DynamoDB → tabela `projections`)

Coloque os `.gz` (baixados do S3) em **`sources/dynamo/projections/dump/prod/`** e rode:
```bash
python sources/dynamo/projections/import_projections_json_prod.py
```
> O `company_id` vem **de dentro do `.gz`** (não precisa passar). Insere em `projections`, move pra `made/prod/`, e roda OPTIMIZE por partição no fim.

Conferir antes sem inserir:
```bash
python sources/dynamo/projections/import_projections_json_prod.py --dry-run --file sources/dynamo/projections/dump/prod/{arquivo}.json.gz --limit 20
```

---

## 3) predictions (DynamoDB → tabela `predictions`)

Coloque os `.gz` em **`sources/dynamo/predictions/dump/prod/`** e rode:
```bash
python sources/dynamo/predictions/import_predictions_json_prod.py
```
> O `company_id` vem **de dentro do `.gz`**. Insere em `predictions`, move pra `made/prod/`.
> A `predictions` **não é particionada** → o OPTIMIZE não é automático. Ao terminar tudo, rode no ClickHouse:
> ```sql
> OPTIMIZE TABLE futurai_db.predictions FINAL;
> ```

Conferir antes:
```bash
python sources/dynamo/predictions/import_predictions_json_prod.py --dry-run --file sources/dynamo/predictions/dump/prod/{arquivo}.json.gz --limit 20
```

---

## 4) variables (DynamoDB → tabela `event_readings`)

Coloque os `.gz` em **`sources/dynamo/variables/dump/prod/`** e rode:
```bash
COMPANY_ID_PROD={company_id} python sources/dynamo/variables/import_variables_json_prod.py
```
> O `.gz` de variables **não tem company_id** → **`COMPANY_ID_PROD` é obrigatório** (sem ele, o script aborta). Uma empresa por vez.
> Insere em `event_readings` (colunas `company_id, tag_id, value, ts` — sem `process_id`), move pra `made/prod/`, e roda OPTIMIZE por partição no fim.

Conferir antes:
```bash
COMPANY_ID_PROD={company_id} python sources/dynamo/variables/import_variables_json_prod.py --dry-run --file sources/dynamo/variables/dump/prod/{arquivo}.json.gz --limit 20
```

---

## 5) export readings (ClickHouse → CSV)

Caminho **inverso** dos outros: puxa dados que já estão no ClickHouse de produção e grava num CSV, filtrando por empresa, período e (opcional) lista de tags. A configuração vem de um **arquivo `.yaml`** (não é mais por linha de comando).

```bash
python sources/clickhouse/export_csv_readings_prod.py             # lê ./export_config.yaml
python sources/clickhouse/export_csv_readings_prod.py meu.yaml    # ou aponta o arquivo
```

Modelo do YAML (veja [export_config.example.yaml](sources/clickhouse/export_config.example.yaml)):
```yaml
COMPANY_ID: 3c77195204214c7caf3012ba77c3633e
DATE_INI: 2026-06-25-11:00        # YYYY-MM-DD-HH:MM (segundos opcionais; padrão :00)
DATE_END: 2026-06-25-12:00
NO_FINAL: false                   # opcional: true = mais rápido, pode duplicar
TAGS:                             # opcional: sem o bloco, traz TODAS as tags
  - CA-H1_CAO
  - CA-H1_AL2O3
```
> **Obrigatórios:** `COMPANY_ID`, `DATE_INI`, `DATE_END`. Data/valor inválido → aborta sem conectar. Período **inclusivo nas duas pontas**.
> **Conexão:** no script do **repo** é **opcional** no YAML — sem o bloco `HOST/USER/…`, usa o `.env` (via `common/config`). Se colocar no YAML, sobrepõe o `.env`.
> **Fuso:** filtra o `ts` **exatamente como está no banco** (UTC = local+3h). Não há conversão — quem consome o CSV faz a conta de cabeça.
> **Formato do CSV (wide):** 1ª coluna `TIMESTAMP` (um período por linha); as demais colunas são cada `tag_id` com seus valores. Separador `;` e UTF-8 com BOM (abre certinho no Excel PT-BR).
> O CSV vai pra `sources/clickhouse/made/prod/` com nome `{company_id}_{início}_a_{fim}.csv`, e cada export é registrado em `output/prod/.recebidos.log`.

**Versão standalone** (`sources/clickhouse/export_csv_readings_standalone.py`) — para enviar a quem faz a importação: mesmo YAML, mas **autossuficiente** (não usa o `common/`; só `pip install clickhouse-connect pandas`). Nela a **conexão vai no YAML** (`HOST/PORT/DATABASE/USER/PASSWORD/SECURE`) e a saída respeita `OUT_DIR`. Modelo: [export_config.standalone.example.yaml](sources/clickhouse/export_config.standalone.example.yaml).

---

## Resumo

| Fluxo | Pasta dos arquivos | Tabela | company_id | OPTIMIZE |
|---|---|---|---|---|
| readings (ADX) | `sources/adx/dump/prod/` (gerado pelo export) | `readings` | `COMPANY_ID_PROD` (obrigatório) | automático (por partição) |
| projections | `sources/dynamo/projections/dump/prod/` | `projections` | de dentro do `.gz` | automático (por partição) |
| predictions | `sources/dynamo/predictions/dump/prod/` | `predictions` | de dentro do `.gz` | manual (`OPTIMIZE TABLE … FINAL`) |
| variables | `sources/dynamo/variables/dump/prod/` | `event_readings` | `COMPANY_ID_PROD` (obrigatório) | automático (por partição) |
| **export readings (→ CSV)** | gera em `sources/clickhouse/made/prod/` | lê de `readings` | `COMPANY_ID` no `.yaml` | não se aplica (só leitura) |

**Comum aos imports (1–4):** arquivo entra em `dump/prod/` → script processa → move pra `made/prod/` → registra em `output/prod/.enviados.log`. Reexecutar só pega o que ainda está em `dump/prod/`. Flags `--dry-run` / `--file` / `--limit` valem para os imports do Dynamo (projections, predictions, variables).

**Export (5):** só leitura — não há `dump`; o CSV é gerado direto em `made/prod/` e cada export é somado em `output/prod/.recebidos.log`.
