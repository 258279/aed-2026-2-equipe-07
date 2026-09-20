# Event Sourcing + CQRS — Agregado Agendamento (Aula 05)

Data: 2026-09-20
Status: aprovado para implementação

## 1. Contexto

O ADR-002 já descreve, como parte do domínio, um caminho de exceção com compensação que nunca foi implementado em código: *"se o cliente cancela dentro do prazo, o sinal é estornado; se não comparece (no-show), uma multa é cobrada"*. Hoje `servico-agendamentos` só implementa o caminho feliz — recebe uma confirmação via HTTP e publica `AgendamentoConfirmadoEvent` no Kafka. Não existe estado persistido, não existe cancelamento, não existe estorno, não existe no-show.

A Aula 05 pede pra aplicar Event Sourcing a UM agregado do domínio que tenha esse tipo de caminho de exceção/auditoria, e derivar dele pelo menos uma projeção CQRS descartável (reconstruível via replay).

## 2. Decisões (do brainstorming)

| Decisão | Escolha |
|---|---|
| Agregado | `Agendamento` — ciclo Confirmado → (Cancelado + SinalEstornado) ou (NaoCompareceu + MultaCobrada) |
| Onde vive o Event Store | Dentro de `servico-agendamentos` (ganha Postgres pela primeira vez) — não um serviço novo |
| Projeção (CQRS) | Relatório de faltas e estornos, contagem por dia — já citado no ADR-002 como "algo que valha reprocessar" |
| Snapshot | Não implementar — volume por stream é pequeno (no máximo 3 eventos) |
| Exposição HTTP | Dois endpoints novos em `AgendamentoController`: `POST /agendamentos/{id}/cancelar` e `POST /agendamentos/{id}/nao-comparecimento` |

## 3. Arquitetura

**Confirmação (caminho existente, adaptado para também gravar no event store):**

```
POST /agendamentos/confirmacoes
        │
        ▼
AgendamentoService.publicarConfirmacao(evento)
        │
        ├─ 1. Publica no Kafka (como já faz hoje — sem mudança)
        │
        └─ 2. AgendamentoEventStore.gravar(agendamentoId, versaoEsperada=1,
                 [AgendamentoConfirmado(v1, payload = o próprio evento)])
```

Este é o único ponto que cria um stream novo (versão 1). `AgendamentoService` passa a depender de `AgendamentoEventStore`, além do `KafkaTemplate` que já usava. As duas escritas (Kafka + event store) não compartilham transação — se a gravação no event store falhar depois do Kafka já ter publicado, a mensagem original ainda existe no tópico (nada se perde do lado do `servico-ocupacao`), mas o agendamento fica sem histórico local até uma correção manual. Aceitável para o escopo desta entrega; não vamos implementar um outbox pattern aqui.

**Cancelamento / não comparecimento (caminho novo):**

```
POST /agendamentos/{id}/cancelar
        │
        ▼
AgendamentoCicloDeVidaService.cancelar(id)
        │
        ├─ 1. AgendamentoEventStore.carregarStream(id)
        │       SELECT * FROM agendamento_eventos WHERE agendamento_id = ? ORDER BY versao
        │
        ├─ 2. Agendamento.reconstruir(eventos)   ← fold dos eventos em memória
        │
        ├─ 3. agregado.cancelar()                ← valida invariante, devolve
        │       [AgendamentoCancelado(v2), SinalEstornado(v3)]
        │
        ├─ 4. AgendamentoEventStore.gravar(id, versaoEsperada=2, novosEventos)
        │       INSERT ... (agendamento_id, versao, tipo_evento, payload)
        │       UNIQUE (agendamento_id, versao) detecta conflito de concorrência
        │
        └─ 5. ProjetorFaltasEstornos.processar(novosEventos)
                UPDATE/INSERT relatorio_faltas_estornos (fora da transação do passo 4)
```

O passo 5 acontece **depois** que o passo 4 já commitou — de propósito. Isso cria a janela real de consistência eventual entre "o fato está gravado" e "o relatório está atualizado", que é o que a Tarefa 4 pede pra documentar. Se o passo 5 falhar, a projeção fica defasada até o próximo replay manual — não há perda do evento (ele já está no event store), só atraso no relatório.

## 4. Componentes novos

### 4.1 `servico-agendamentos/pom.xml`, `application.properties` e infraestrutura

Adiciona `spring-boot-starter-jdbc` + `postgresql` (runtime) — mesmas coordenadas já usadas em `servico-ocupacao/pom.xml` (confirmado: não renomeado no Spring Boot 4.1.0).

**Topologia de banco:** um único container Postgres continua sendo suficiente (não criar um segundo container). O `servico-agendamentos` ganha seu **próprio banco** dentro do mesmo container — `agendamentos`, separado do banco `ocupacao` que já existe — mantendo cada serviço dono do seu próprio schema, sem os dois escreverem nas mesmas tabelas.

Mudanças concretas em `docker-compose.yml`:
- Novo arquivo `docker/postgres-init/init-agendamentos.sql`, montado no mesmo container Postgres (a imagem oficial roda todo `.sql` dentro de `docker-entrypoint-initdb.d/` em ordem — múltiplos arquivos convivem sem conflito):
  ```sql
  CREATE DATABASE agendamentos;
  \c agendamentos
  CREATE TABLE agendamento_eventos (...);
  CREATE TABLE relatorio_faltas_estornos (...);
  ```
- No serviço `servico-agendamentos` do `docker-compose.yml`: adicionar `depends_on: postgres` e `SPRING_DATASOURCE_URL=jdbc:postgresql://postgres:5432/agendamentos` (+ usuário/senha, iguais aos já usados por `servico-ocupacao`).

### 4.2 `domain/EventoAgendamento.java` (registro único, usado nas duas direções)

Um tipo simples carregando `agendamentoId`, `versao`, `tipoEvento`, `payload` (JSON como String), `ocorridoEm`. É o mesmo tipo em todo o fluxo — não existe um tipo `EventoNovo` separado: quando o agregado (`Agendamento.cancelar()`, etc.) precisa devolver eventos para gravar, ele já devolve `EventoAgendamento` com `versao` preenchida (calculada a partir de `versaoAtual + 1`, `+2`, ...) e `ocorridoEm` preenchida (o agregado usa a hora fornecida no comando, não gera sozinho). `AgendamentoEventStore.gravar` recebe essa mesma lista e só insere; `ProjetorFaltasEstornos.processar` recebe a mesma lista também, seja vinda de uma gravação recente ou de um replay completo do banco.

### 4.3 `domain/Agendamento.java` (o agregado)

```java
Agendamento.reconstruir(List<EventoAgendamento> eventos) -> Agendamento
agendamento.cancelar() -> List<EventoNovo>       // valida: só se status == CONFIRMADO
agendamento.marcarNaoComparecimento() -> List<EventoNovo>  // valida: só se status == CONFIRMADO
```

Estado interno: `agendamentoId`, `status` (CONFIRMADO / CANCELADO / NAO_COMPARECEU), `versaoAtual`. Nenhuma dependência de Spring/JDBC aqui — é POJO puro, testável sem banco.

### 4.4 `service/AgendamentoEventStore.java`

```java
List<EventoAgendamento> carregarStream(String agendamentoId)
void gravar(String agendamentoId, int versaoEsperada, List<EventoNovo> eventos)
```

`gravar` insere cada evento com `versaoEsperada`, `versaoEsperada+1`, ... dentro de uma transação. Se a constraint `UNIQUE (agendamento_id, versao)` for violada (`DataIntegrityViolationException` do Spring JDBC), a exceção é traduzida para `ConflitoDeVersaoException` — o sinal de "alguém já escreveu essa versão antes de você".

### 4.5 `service/AgendamentoCicloDeVidaService.java`

Orquestra os passos 1-5 do diagrama acima para `cancelar(id)` e `marcarNaoComparecimento(id)`. É aqui, e só aqui, que mora a decisão "grava o evento, depois atualiza a projeção, nessa ordem".

### 4.6 `controller/AgendamentoController.java` (modificado)

Dois métodos novos: `POST /{id}/cancelar` e `POST /{id}/nao-comparecimento`. Mapeiam `ConflitoDeVersaoException` para `409 Conflict`; estado inválido (ex. cancelar algo já cancelado) também vira `409 Conflict` — mesmo código para os dois casos, porque ambos são "o estado do agregado não é o que você esperava", só que um vem de concorrência e o outro de regra de negócio.

### 4.7 `service/ProjetorFaltasEstornos.java`

```java
void processar(List<EventoAgendamento> eventos)     // incremental, chamado após gravar
void reconstruirDoZero()                              // TRUNCATE + replay de TODO o event store
```

`processar` reage só a `SinalEstornado` (incrementa `ESTORNO` do dia) e `MultaCobrada` (incrementa `MULTA` do dia) — esses dois são os eventos "terminais" de cada caminho, evitando contar cancelamento+estorno como dois casos.

`reconstruirDoZero` apaga a tabela `relatorio_faltas_estornos` inteira e reprocessa **todos** os eventos de **todos** os streams do event store, na ordem, através do mesmo `processar`. É o método que o teste de replay chama.

### 4.8 Tabela `relatorio_faltas_estornos`

`data` (VARCHAR/DATE), `tipo` (ESTORNO ou MULTA), `quantidade` (INT), `PRIMARY KEY (data, tipo)` — mesmo padrão de upsert-por-chave já usado em `agregacao_confirmacoes_por_janela`.

## 5. Testes

- `AgendamentoTest` (unitário, sem Spring): fold de eventos reconstrói o estado certo; `cancelar()` numa `Agendamento` já cancelada lança exceção de invariante.
- `AgendamentoEventStoreTest` (`@SpringBootTest`, H2): grava stream, recarrega, confirma ordem e versões; grava duas vezes a mesma versão → `ConflitoDeVersaoException`.
- `AgendamentoCicloDeVidaServiceTest`: cancelar um agendamento confirmado grava os 2 eventos certos e atualiza a projeção.
- **`ProjetorFaltasEstornosReplayTest` — o teste mais importante da entrega**: popula o event store com uma mistura de confirmações, cancelamentos+estornos e no-shows+multas; deixa a projeção se popular normalmente; anota os números; **apaga a tabela da projeção**; chama `reconstruirDoZero()`; confirma que os números batem exatamente com os de antes.

## 6. Documentação

- `docs/adr/ADR-005-event-sourcing.md` — 5 seções pedidas pela tarefa (contexto, decisão, alternativas, consequências aceitas incluindo LGPD/crypto-shredding, status).
- `docs/entregas/aula-05.md` — como rodar, e a defasagem tolerada da projeção (relatório gerencial, não tela em tempo real — segundos de atraso são aceitáveis, justificado pela ordem gravar-depois-projetar do item 3).
- `docs/IA.md` — nova entrada, incluindo pelo menos uma sugestão da IA recusada de propósito (ex: a IA sugerir `UPDATE` no event store pra "corrigir" um evento, ou sugerir Event Sourcing pra outro agregado que não precisa).

## 7. Fora de escopo

- Fluxo de "solicitação" antes da confirmação (não existe hoje, não será criado).
- Saga (conteúdo da aula 06).
- Publicar os novos eventos (`AgendamentoCancelado` etc.) no Kafka — o event store desta entrega é local a `servico-agendamentos`; integração com `servico-ocupacao` via Kafka fica fora, a não ser que uma tarefa futura peça.
- Snapshot do agregado.
- Tag de entrega — o usuário cria manualmente depois de revisar.
