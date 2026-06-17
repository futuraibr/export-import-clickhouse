# Anotações — verificação predictions / projections / readings (dev)

Contexto: queria ver se o mesmo que achei na readings de produção (duplicação e dado
de hora em hora) também acontece nas predictions e projections. Olhei na tabela de dev,
que só tem o company_id `a57d9b153ff144d9a2b6e7e8e3a04dc3`. Detalhe: no dev não
tem nada do dia 12/06 pra frente — os dados vão de 15/05 a 21/05, então testei nessa
janela mesmo. Não alterei nada, só consultei.

---

READINGS — TESTE DEV (15 a 21/05)
-> Aconteceu o mesmo que na Votorantim, mas pela metade: os dados estão sendo puxados
certinho de minuto em minuto, só que TODO ponto da hora cheia (HH:00:00) está duplicado.
Peguei como exemplo a tag ARA-384F922.PV e a company a57d9b153ff144d9a2b6e7e8e3a04dc3:
o ponto das 2026-05-21 10:00:00 aparece duas vezes, com o valor idêntico (29.0515575 nas
duas), ou seja, não é média, é cópia mesmo. E não é só essa tag — são 3450 linhas
duplicadas no total, 100% delas caindo na virada da hora, e atinge todas as 23 tags.
Acontece o tempo todo, de 15 a 21/05. Diferente da Votorantim, aqui a base continua de
minuto em minuto; o que duplica é só o ponto do :00.

PREDICTIONS — TESTE DEV (15 a 21/05)
-> Aparentemente está tudo correto. Testei no processo 118692dce1a34e2d8e6b3e50beb36601
e company a57d9b153ff144d9a2b6e7e8e3a04dc3, está puxando de minuto em minuto e não está
duplicando nada (5.824 linhas e 5.824 chaves únicas, bateu certinho). Mesma situação da
Klabin, sem problema.

PROJECTIONS — TESTE DEV (15 a 21/05)
-> Também está correto. Testei no mesmo processo 118692dce1a34e2d8e6b3e50beb36601 com a
tag ARA-384V984.PV, de minuto em minuto e sem duplicar (11.104 linhas, todas com chave
única). Sem problema aqui também.

ATENÇÃO AQUI: o problema só apareceu na readings. As predictions e projections em dev
estão limpas. E o padrão da readings é bem específico — duplica só o ponto da hora cheia,
sempre com o valor igual ao do minuto :00 — o que parece mais um insert do mesmo ponto
repetido na virada da hora (algum job horário reescrevendo o :00, ou a ingestão de minuto
batendo com uma de hora) do que "salvar média de hora em hora".

---

Pendências / o que ainda não dá pra afirmar:
- Confirmar nas empresas de produção (Klabin/Votorantim/CSN).
- No dev não tem dado de 12/06+; se aparecer, vale repetir o teste nessa data pra bater com prod.
