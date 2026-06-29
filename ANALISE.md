# Análise — ClickHouse de PRODUÇÃO (readings / predictions / projections)

Verificação feita direto no ClickHouse de produção, **só leitura** (nada foi alterado).
Olhei as 3 tabelas, todas as companies, foco em 12/06+. Refeita em 18/06.

## ⚠️ CORREÇÃO IMPORTANTE (em relação à 1ª versão desta análise)

As três tabelas (`readings`, `predictions`, `projections`) são **`ReplacingMergeTree`** —
eu tinha chutado que projections era MergeTree comum, e estava errado.

Isso muda a conclusão: a `ReplacingMergeTree` **guarda linhas repetidas nos pedaços ainda
não mesclados e colapsa elas na leitura com `FINAL`** (ou quando o merge em background
roda). Então **no resultado das consultas (com FINAL / pós-merge) NÃO há duplicata** — por
isso você não achou nada manualmente. Você estava certo.

O que eu tinha contado antes ("~32M / 46% duplicado") eram as **cópias físicas pré-merge**,
que esse engine existe justamente pra absorver. Não é um erro de dado.

**Prova (projections, Votorantim, dia 18):**
- `count()` normal = 684.496 | com `FINAL` = 396.520 → ~288 mil cópias somem com FINAL.
- Uma chave de exemplo (proc `38c36615`, tag `VI-W1V34F1`, ts `02:11`) tinha 4 linhas;
  com `FINAL` volta **1 linha**.

## Como está cada tabela

- **projections** → sem duplicata no resultado (com FINAL). Engine ReplacingMergeTree
  cuidando. De minuto em minuto.
- **readings** → de minuto em minuto, sem duplicata efetiva (também ReplacingMergeTree).
- **predictions** → de minuto em minuto, sem duplicata efetiva.
- **Granularidade**: tudo de minuto em minuto, em todas as companies. **Nada de hora em
  hora** no ClickHouse (o "hora em hora" que aparecia no DynamoDB/S3 não se reflete aqui).

## O que sobra de real (bem menos grave do que parecia)

1. **A ingestão reinsere o mesmo ponto várias vezes** (2-4×). Isso não quebra consulta
   (o Replacing limpa na leitura/merge), mas gera **churn de storage e de merge**: na
   projections ~46% das linhas físicas são cópias pré-merge (~32M de ~71M). Vale entender
   por que a ingestão reinsere tanto.
2. Das chaves repetidas, **~88% são cópias idênticas** e **~12% têm valor diferente** pro
   mesmo (process, tag, ts). Como a tabela **não tem coluna de versão**, o ReplacingMergeTree
   mantém "a última inserida" nesses casos — provavelmente é **re-projeção** atualizando o
   valor (comportamento esperado), mas sem versão a escolha de qual fica é pela ordem do
   merge.

## Como conferir (do jeito certo, com FINAL)

```sql
-- engine (confirma que é ReplacingMergeTree)
SELECT engine FROM system.tables WHERE database='futurai_db' AND name='projections';

-- normal x FINAL: a diferença são as cópias pré-merge
SELECT count() FROM futurai_db.projections WHERE company_id='<id>' AND ts>='2026-06-18';
SELECT count() FROM futurai_db.projections FINAL WHERE company_id='<id>' AND ts>='2026-06-18';

-- um ponto específico com FINAL deve voltar 1 linha
SELECT * FROM futurai_db.projections FINAL
WHERE company_id='<id>' AND process_id='<p>' AND tag_id='<tag>' AND ts='<ts>';
```

## Pendências / próximos passos
- (Eficiência, não correção) Investigar por que a ingestão da projections reinsere cada
  ponto 2-4×. Se for re-projeção atualizando valor, considerar uma **coluna de versão** no
  ReplacingMergeTree pra a dedup ser determinística (sempre a mais nova).
- Mesma ressalva vale pra análise antiga do dev (`ANOTACOES.md`): se aquele readings também
  for ReplacingMergeTree, a "duplicação na hora cheia" que anotei lá provavelmente também
  some com FINAL — vale reconferir com FINAL.
- Não mexi em nada (só SELECT).
