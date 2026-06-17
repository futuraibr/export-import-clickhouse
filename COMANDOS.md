# Comandos de uso

Referência rápida dos comandos de cada fluxo, em **dev** e **prod**.
Rode tudo a partir da **raiz do projeto** (`migration/`).

> **Antes de tudo:** ativar o ambiente e ter o `.env` preenchido.
> ```bash
> source .venv/bin/activate
> ```
> - **dev** lê as variáveis normais do `.env` (`CH_HOST`, etc.) e usa as subpastas `dev/`.
> - **prod** lê as variáveis `*_PROD` do `.env` (`CH_HOST_PROD`, etc.) e usa as subpastas `prod/` — preencha antes.

Dev e prod têm **pastas separadas**, então nunca se misturam.

---

## Predictions

- **dev** → arquivos em `sources/dynamo/predictions/dump/dev/`
- **prod** → arquivos em `sources/dynamo/predictions/dump/prod/`

### Dev (ClickHouse de dev)
```bash
# 1. validar 1 arquivo (não insere, não move)
python sources/dynamo/predictions/import_predictions_json.py --dry-run --file sources/dynamo/predictions/dump/dev/<arquivo>.json.gz --limit 20

# 2. carga real (insere, move pra made/dev/, roda OPTIMIZE no fim)
python sources/dynamo/predictions/import_predictions_json.py
```

### Prod (ClickHouse de prod)
```bash
# 1. validar 1 arquivo
python sources/dynamo/predictions/import_predictions_json_prod.py --dry-run --file sources/dynamo/predictions/dump/prod/<arquivo>.json.gz --limit 20

# 2. carga real
python sources/dynamo/predictions/import_predictions_json_prod.py
```

---

## Projections

- **dev** → arquivos em `sources/dynamo/projections/dump/dev/`
- **prod** → arquivos em `sources/dynamo/projections/dump/prod/`

### Dev (ClickHouse de dev)
```bash
# 1. validar 1 arquivo
python sources/dynamo/projections/import_projections_json.py --dry-run --file sources/dynamo/projections/dump/dev/<arquivo>.json.gz --limit 20

# 2. carga real
python sources/dynamo/projections/import_projections_json.py
```

### Prod (ClickHouse de prod)
```bash
# 1. validar 1 arquivo
python sources/dynamo/projections/import_projections_json_prod.py --dry-run --file sources/dynamo/projections/dump/prod/<arquivo>.json.gz --limit 20

# 2. carga real
python sources/dynamo/projections/import_projections_json_prod.py
```

### Flags úteis (predictions e projections)
- `--dry-run` — mostra como ficaria, sem inserir nada
- `--file <arquivo>` — processa só um arquivo (bom pra conferir antes)
- `--limit N` — processa só os N primeiros itens
- `--no-optimize` — não roda o `OPTIMIZE TABLE ... FINAL` no fim

---

## ADX (bônus — para completar o quadro)

Duas etapas: **exportar** (ADX → CSV em `dump/<ambiente>`) e **importar** (CSV → ClickHouse).

### Dev
```bash
python sources/adx/export_adx.py            # puxa do ADX dev → dump/dev/
python sources/adx/import_clickhouse.py     # sobe os CSVs no ClickHouse dev
```

### Prod
```bash
# preencher antes no .env: ADX_*_PROD e CH_*_PROD
python sources/adx/export_adx_prod.py       # puxa do ADX prod → dump/prod/
python sources/adx/import_clickhouse_prod.py# sobe os CSVs no ClickHouse prod
```

---

## Como funciona (resumo)

- Cada fluxo tem, dentro da pasta do script, **3 pastas** (`dump`, `made`, `output`),
  e cada uma tem as subpastas **`dev/`** e **`prod/`**.
- O script **dev** mexe só nas `.../dev`; o **prod** mexe só nas `.../prod`. Não tem como misturar.
- Fluxo: arquivo entra em `dump/<amb>` → script processa → move pra `made/<amb>` → registra em
  `output/<amb>/.enviados.log`. Reexecutar só pega o que ainda está em `dump/<amb>`.
- **Sempre valide com `--dry-run` antes da carga real**, principalmente em prod.
- O nome da tabela é o mesmo em dev e prod (`predictions`, `projections`, `readings`);
  o que muda é o **banco/host** (definido pelas `CH_*` vs `CH_*_PROD`).
