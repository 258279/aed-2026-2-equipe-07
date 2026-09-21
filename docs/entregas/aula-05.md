# Aula 05 — Event Sourcing e CQRS

## Como rodar

1. **Importante se você já subiu o Postgres deste projeto antes:** os scripts em
   `docker/postgres-init/` só rodam quando o volume de dados do Postgres está vazio —
   se o volume `postgres_data` já existe de uma vez anterior (bem provável, já que
   `servico-ocupacao` usa Postgres desde a Aula 04), o banco `agendamentos` novo
   **não** vai ser criado automaticamente, e `servico-agendamentos` vai falhar ao
   conectar. Recrie o volume antes de subir pela primeira vez com o `agendamentos`:
   ```bash
   docker compose down -v
   ```
2. Subir a infraestrutura (agora com dois bancos no mesmo container Postgres —
   `ocupacao` e `agendamentos`):
   ```bash
   docker compose up --build
   ```
3. Confirmar um agendamento (grava a versão 1 do stream e publica no Kafka):
   ```bash
   curl -X POST http://localhost:8080/agendamentos/confirmacoes \
     -H "Content-Type: application/json" \
     -d '{"eventoId":"EVT-1","agendamentoId":"AG-900","profissionalId":"PROF-1","servicoId":"SERV-1","inicioEm":"2026-09-20T14:00:00-03:00","prioridade":"PADRAO","ocorridoEm":"2026-09-20T13:00:00-03:00"}'
   ```
4. Cancelar (grava `AgendamentoCancelado` + `SinalEstornado`, e atualiza a projeção):
   ```bash
   curl -X POST http://localhost:8080/agendamentos/AG-900/cancelar
   ```
5. Marcar não comparecimento (em outro agendamento, sem cancelar antes):
   ```bash
   curl -X POST http://localhost:8080/agendamentos/AG-901/nao-comparecimento
   ```
6. Consultar a projeção diretamente no banco:
   ```bash
   docker exec -it <container_postgres> psql -U user -d agendamentos \
     -c "SELECT * FROM relatorio_faltas_estornos;"
   ```
7. Reconstruir a projeção do zero (via teste, que é a evidência formal exigida):
   ```bash
   cd servico-agendamentos && ./mvnw test -Dtest=ProjetorFaltasEstornosReplayTest
   ```

## Defasagem tolerada, por tela

| Tela / funcionalidade | Defasagem tolerada | Por quê |
|---|---|---|
| Confirmar / cancelar / marcar não comparecimento (escrita) | Nenhuma — é síncrono | O event store é a fonte da verdade; a resposta HTTP só volta depois do `INSERT` confirmado. Não é uma leitura, então não há consistência eventual aqui. |
| Relatório de faltas e estornos (`relatorio_faltas_estornos`) | Segundos, aceitável | É um relatório gerencial — ninguém toma uma decisão operacional no exato milissegundo em que um cancelamento acontece. A projeção é atualizada logo depois do evento ser gravado (não na mesma transação), então na pior hipótese um relatório consultado no mesmo segundo de um cancelamento pode não refletir esse cancelamento ainda. Isso é aceitável porque o relatório serve para tendência ao longo do dia/semana, não para uma ação imediata. |

Se o passo de atualizar a projeção falhar depois do evento já ter sido gravado, a
projeção fica defasada até alguém rodar `reconstruirDoZero()` manualmente — não há
perda de dado (o evento está no event store), só atraso no relatório. Não construímos
um mecanismo automático de recuperação porque o volume desta entrega não justifica —
fica registrado como próximo passo caso o relatório vire algo consultado com mais
frequência.
