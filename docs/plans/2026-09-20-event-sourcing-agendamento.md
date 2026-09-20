# Event Sourcing + CQRS — Agregado Agendamento Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Aplicar Event Sourcing ao agregado `Agendamento` em `servico-agendamentos` (confirmação → cancelamento+estorno ou não-comparecimento+multa), com um Event Store append-only, ordenado e versionado por stream, e derivar dele uma projeção CQRS (relatório de faltas/estornos) reconstruível do zero via replay.

**Architecture:** Um agregado POJO (`Agendamento`) reconstrói estado por fold dos eventos; um `AgendamentoEventStore` (JdbcTemplate + `PRIMARY KEY (agendamento_id, versao)`) grava e carrega streams, traduzindo violação de chave em conflito de concorrência; um `ProjetorFaltasEstornos` mantém uma tabela derivada e sabe reconstruí-la do zero.

**Tech Stack:** Spring Boot 4.1.0, Spring JDBC (`JdbcTemplate`), Postgres (novo banco `agendamentos`, mesmo container), H2 para testes, Jackson 3 (`tools.jackson`), JUnit 5, Mockito.

**Spec:** `docs/superpowers/specs/2026-09-20-event-sourcing-agendamento-design.md`

---

## Antes de começar

1. **`servico-agendamentos` hoje não tem banco nenhum.** Confirmado no `pom.xml`: só `spring-boot-starter-webmvc` e `spring-boot-starter-kafka`. A Parte 1 adiciona JDBC/Postgres pela primeira vez nesse módulo.
2. **`JsonMapper.writeValueAsString(Object)`** (Task 5) é assumido disponível em `tools.jackson.databind.json.JsonMapper`, herdado de `ObjectMapper` — API estável há anos no Jackson 2.x, e este projeto já usa `JsonMapper.builder().build()` e `.readValue(...)` em `servico-ocupacao`. Se o nome do método não bater ao compilar, procure o equivalente na mesma classe (não deve ter mudado de nome no Jackson 3).
3. **`spring-boot-starter-webmvc` já traz Jackson transitivamente** (Spring MVC precisa de JSON pra content negotiation), então um bean `JsonMapper` deve já estar autoconfigurado. Se a injeção falhar com `NoSuchBeanDefinitionException`, adicione `spring-boot-starter-json` explicitamente ao `pom.xml` (mesma correção que já foi necessária em outros pontos deste projeto).
4. **Duas partes, cada uma entregando algo que compila e testa sozinho:** Parte 1 (Tasks 1-7) entrega o Event Store funcionando de ponta a ponta via HTTP (confirmar, cancelar, marcar não comparecimento), sem projeção ainda. Parte 2 (Tasks 8-11) adiciona a projeção CQRS, o teste de replay, e a documentação.
5. Nenhum passo deste plano cria a tag de entrega — isso fica por conta do usuário, manualmente, depois de revisar tudo.

---

# PARTE 1 — ADR-005 + Event Store (Tarefas 1 e 2 do enunciado)

### Task 1: `docs/adr/ADR-005-event-sourcing.md`

**Files:**
- Create: `docs/adr/ADR-005-event-sourcing.md`

Documentação pura, sem TDD.

- [ ] **Step 1: Escrever o ADR**

```markdown
# ADR-005 — Event Sourcing no agregado Agendamento

## Status

Aceito

## Contexto

O ADR-002 já identificou, como parte do domínio, um caminho de exceção com compensação
que nunca foi implementado em código: cancelamento dentro do prazo devolve o sinal
pago; não comparecimento (no-show) gera cobrança de multa. Esse caminho precisa de
rastreabilidade real — não basta saber o estado final ("cancelado"), é preciso saber
a sequência exata de fatos que levou a ele, para auditoria e para responder perguntas
sobre o passado que podem mudar de interpretação com o tempo (ex.: uma disputa sobre
se o cancelamento aconteceu dentro do prazo).

## Decisão

Aplicar Event Sourcing ao agregado `Agendamento`, especificamente ao trecho do seu
ciclo de vida que vai de `Confirmado` até `Cancelado + SinalEstornado` ou
`NaoCompareceu + MultaCobrada`. O estado do agregado deixa de ser gravado diretamente
e passa a ser reconstruído por fold da sequência de eventos gravados em um Event Store
próprio (`agendamento_eventos`), append-only, ordenado e versionado por stream
(`agendamento_id`).

Nenhum outro agregado do sistema (a projeção de ocupação, o agregador por janela) é
afetado — Event Sourcing é aplicado só aqui, de propósito.

## Alternativas consideradas

- **CRUD tradicional (UPDATE no estado do agendamento)**: descartado porque perde a
  sequência de fatos — depois de um UPDATE, não há como reconstruir "o que aconteceu
  e quando", só o estado final. Não sustenta auditoria nem disputa sobre prazo de
  cancelamento.
- **Log de auditoria separado, ao lado de uma tabela de estado tradicional**: melhora
  a rastreabilidade, mas cria duas fontes de verdade que podem divergir (o log diz uma
  coisa, o estado gravado diz outra, e nada força consistência entre os dois). Event
  Sourcing elimina essa divergência ao fazer do log a única fonte de verdade.

## Consequências aceitas

- **Evolução de esquema (upcasting)**: se o formato de um evento precisar mudar no
  futuro (ex.: adicionar um campo em `SinalEstornado`), eventos antigos já gravados
  continuam no formato velho para sempre — o código que faz fold precisa saber ler
  as duas versões, ou um processo de upcasting precisa traduzir o formato antigo pro
  novo na leitura.
- **Crescimento do log**: o Event Store só cresce, nunca encolhe. Para o volume deste
  domínio (no máximo 3 eventos por agendamento) isso não é um problema agora, mas em
  escala maior exigiria arquivamento de streams antigos.
- **LGPD (direito à eliminação)**: um log append-only não pode simplesmente apagar o
  dado pessoal de um cliente sob pedido, sem quebrar a integridade do histórico. A
  saída aceita é **crypto-shredding**: dado pessoal sensível no payload seria
  criptografado com uma chave por cliente; "esquecer" o cliente significa jogar fora
  a chave, não o evento — o evento continua existindo, mas ilegível.
- **Curva de aprendizado da equipe**: reconstruir estado por fold em vez de ler um
  campo direto do banco é um modelo mental novo, mais custoso de debugar (é preciso
  reproduzir a sequência de eventos pra entender um estado, não só olhar uma linha de
  tabela). Aceito porque só um agregado pequeno usa esse modelo — o resto do sistema
  continua CRUD simples, o que os revisores confirmaram ser um acerto de escopo, não
  dívida técnica.
```

- [ ] **Step 2: Commit**

```bash
git add docs/adr/ADR-005-event-sourcing.md
git commit -m "docs: adiciona ADR-005 sobre Event Sourcing no agregado Agendamento"
```

---

### Task 2: Infraestrutura — Postgres para `servico-agendamentos`

**Files:**
- Modify: `servico-agendamentos/pom.xml`
- Modify: `docker-compose.yml`
- Create: `docker/postgres-init/init-agendamentos.sql`

Nenhuma mudança em `application.properties` é necessária — sem `spring.datasource.url` configurado ali, o Spring Boot detecta o H2 (dependência de teste) automaticamente pros testes, e o `docker-compose.yml` fornece a URL real em runtime via variável de ambiente. É o mesmo padrão já usado em `servico-ocupacao`.

Infraestrutura pura — sem TDD, mas com verificação de que o módulo ainda compila e os testes existentes continuam passando no final.

- [ ] **Step 1: Adicionar dependências ao `servico-agendamentos/pom.xml`**

Dentro de `<dependencies>`, adicionar (mesmas coordenadas já usadas em `servico-ocupacao/pom.xml`):

```xml
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-jdbc</artifactId>
        </dependency>
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-jdbc-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>com.h2database</groupId>
            <artifactId>h2</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-json</artifactId>
        </dependency>
```

`spring-boot-starter-json` é adicionado proativamente aqui (não só se der erro depois): `servico-ocupacao` precisou dessa mesma dependência explícita mesmo já tendo Jackson transitivo por outro caminho, e a Task 5 deste plano depende de injetar um bean `JsonMapper` — mais barato resolver agora do que interromper o fluxo de TDD da Task 5 pra descobrir a mesma coisa.

- [ ] **Step 2: Criar o script de inicialização do novo banco**

Criar `docker/postgres-init/init-agendamentos.sql`:

```sql
CREATE DATABASE agendamentos;

\c agendamentos

CREATE TABLE IF NOT EXISTS agendamento_eventos (
    agendamento_id VARCHAR(100) NOT NULL,
    versao INTEGER NOT NULL,
    tipo_evento VARCHAR(60) NOT NULL,
    payload TEXT NOT NULL,
    ocorrido_em VARCHAR(40) NOT NULL,
    gravado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (agendamento_id, versao)
);
```

A imagem oficial do Postgres roda todo `.sql` dentro de `docker-entrypoint-initdb.d/` em ordem alfabética — esse arquivo convive com o `init.sql` já existente (que continua cuidando só do banco `ocupacao`), sem conflito, porque cada um cria e usa seu próprio banco.

- [ ] **Step 3: Apontar `servico-agendamentos` pro novo banco, no `docker-compose.yml`**

No bloco `servico-agendamentos`, trocar:

```yaml
  servico-agendamentos:
    build:
      context: ./servico-agendamentos
    environment:
      - SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka:9092
    depends_on:
      - kafka
    ports:
      - "8080:8080"
```

por:

```yaml
  servico-agendamentos:
    build:
      context: ./servico-agendamentos
    environment:
      - SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka:9092
      - SPRING_DATASOURCE_URL=jdbc:postgresql://postgres:5432/agendamentos
      - SPRING_DATASOURCE_USERNAME=user
      - SPRING_DATASOURCE_PASSWORD=pass
    depends_on:
      - kafka
      - postgres
    ports:
      - "8080:8080"
```

- [ ] **Step 4: Rodar a suíte de testes existente pra garantir que nada quebrou**

Run: `cd servico-agendamentos && ./mvnw test`
Expected: PASS em todos os testes já existentes (nenhum código de produção foi alterado ainda nesta task, só dependências e infra)

- [ ] **Step 5: Commit**

```bash
git add servico-agendamentos/pom.xml servico-agendamentos/src/main/resources/application.properties \
        docker-compose.yml docker/postgres-init/init-agendamentos.sql
git commit -m "feat: adiciona Postgres ao servico-agendamentos para o event store"
```

---

### Task 3: O agregado — `EventoAgendamento` e `Agendamento`

**Files:**
- Create: `servico-agendamentos/src/main/java/br/pucminas/aed/equipe07/agendamentos/domain/EventoAgendamento.java`
- Create: `servico-agendamentos/src/main/java/br/pucminas/aed/equipe07/agendamentos/domain/Agendamento.java`
- Test: `servico-agendamentos/src/test/java/br/pucminas/aed/equipe07/agendamentos/domain/AgendamentoTest.java`

Este é o coração do Event Sourcing: uma classe de domínio pura, sem Spring, sem banco — só fold de eventos e regras de negócio. Por isso o TDD aqui é direto: escrever o teste, ver falhar (a classe nem existe), implementar, ver passar.

- [ ] **Step 1: Escrever o teste que ainda vai falhar**

```java
package br.pucminas.aed.equipe07.agendamentos.domain;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AgendamentoTest {

    @Test
    void deveReconstruirComoConfirmadoAPartirDoEventoDeConfirmacao() {

        Agendamento agendamento = Agendamento.reconstruir(List.of(
                new EventoAgendamento("AG-700", 1, Agendamento.TIPO_CONFIRMADO, "{}", "2026-09-01T10:00:00-03:00")
        ));

        assertEquals(Agendamento.Status.CONFIRMADO, agendamento.getStatus());
        assertEquals(1, agendamento.getVersaoAtual());
    }

    @Test
    void deveGerarCanceladoESinalEstornadoAoCancelarUmConfirmado() {

        Agendamento agendamento = Agendamento.reconstruir(List.of(
                new EventoAgendamento("AG-701", 1, Agendamento.TIPO_CONFIRMADO, "{}", "2026-09-01T10:00:00-03:00")
        ));

        List<EventoAgendamento> novosEventos = agendamento.cancelar("2026-09-02T09:00:00-03:00");

        assertEquals(2, novosEventos.size());
        assertEquals(2, novosEventos.get(0).getVersao());
        assertEquals(Agendamento.TIPO_CANCELADO, novosEventos.get(0).getTipoEvento());
        assertEquals(3, novosEventos.get(1).getVersao());
        assertEquals(Agendamento.TIPO_SINAL_ESTORNADO, novosEventos.get(1).getTipoEvento());
    }

    @Test
    void deveGerarNaoCompareceuEMultaCobradaAoMarcarNaoComparecimento() {

        Agendamento agendamento = Agendamento.reconstruir(List.of(
                new EventoAgendamento("AG-703", 1, Agendamento.TIPO_CONFIRMADO, "{}", "2026-09-01T10:00:00-03:00")
        ));

        List<EventoAgendamento> novosEventos = agendamento.marcarNaoComparecimento("2026-09-01T18:00:00-03:00");

        assertEquals(2, novosEventos.size());
        assertEquals(Agendamento.TIPO_NAO_COMPARECEU, novosEventos.get(0).getTipoEvento());
        assertEquals(Agendamento.TIPO_MULTA_COBRADA, novosEventos.get(1).getTipoEvento());
    }

    @Test
    void naoDevePermitirCancelarUmAgendamentoJaCancelado() {

        Agendamento agendamento = Agendamento.reconstruir(List.of(
                new EventoAgendamento("AG-702", 1, Agendamento.TIPO_CONFIRMADO, "{}", "2026-09-01T10:00:00-03:00"),
                new EventoAgendamento("AG-702", 2, Agendamento.TIPO_CANCELADO, "{}", "2026-09-02T09:00:00-03:00"),
                new EventoAgendamento("AG-702", 3, Agendamento.TIPO_SINAL_ESTORNADO, "{}", "2026-09-02T09:00:00-03:00")
        ));

        assertThrows(IllegalStateException.class, () -> agendamento.cancelar("2026-09-03T09:00:00-03:00"));
    }
}
```

- [ ] **Step 2: Rodar e confirmar que falha**

Run: `cd servico-agendamentos && ./mvnw test -Dtest=AgendamentoTest`
Expected: FAIL — erro de compilação, `Agendamento` e `EventoAgendamento` ainda não existem.

- [ ] **Step 3: Criar `EventoAgendamento`**

```java
package br.pucminas.aed.equipe07.agendamentos.domain;

public final class EventoAgendamento {

    private final String agendamentoId;
    private final int versao;
    private final String tipoEvento;
    private final String payload;
    private final String ocorridoEm;

    public EventoAgendamento(
            String agendamentoId,
            int versao,
            String tipoEvento,
            String payload,
            String ocorridoEm) {

        this.agendamentoId = agendamentoId;
        this.versao = versao;
        this.tipoEvento = tipoEvento;
        this.payload = payload;
        this.ocorridoEm = ocorridoEm;
    }

    public String getAgendamentoId() {
        return agendamentoId;
    }

    public int getVersao() {
        return versao;
    }

    public String getTipoEvento() {
        return tipoEvento;
    }

    public String getPayload() {
        return payload;
    }

    public String getOcorridoEm() {
        return ocorridoEm;
    }
}
```

- [ ] **Step 4: Criar `Agendamento`**

```java
package br.pucminas.aed.equipe07.agendamentos.domain;

import java.util.List;

public final class Agendamento {

    public static final String TIPO_CONFIRMADO = "AgendamentoConfirmado";
    public static final String TIPO_CANCELADO = "AgendamentoCancelado";
    public static final String TIPO_SINAL_ESTORNADO = "SinalEstornado";
    public static final String TIPO_NAO_COMPARECEU = "AgendamentoNaoCompareceu";
    public static final String TIPO_MULTA_COBRADA = "MultaCobrada";

    public enum Status { CONFIRMADO, CANCELADO, NAO_COMPARECEU }

    private final String agendamentoId;
    private final Status status;
    private final int versaoAtual;

    private Agendamento(String agendamentoId, Status status, int versaoAtual) {
        this.agendamentoId = agendamentoId;
        this.status = status;
        this.versaoAtual = versaoAtual;
    }

    public static Agendamento reconstruir(List<EventoAgendamento> eventos) {

        if (eventos.isEmpty()) {
            throw new IllegalStateException("Nenhum evento encontrado para este agendamento");
        }

        String agendamentoId = eventos.get(0).getAgendamentoId();
        Status status = null;
        int versaoAtual = 0;

        for (EventoAgendamento evento : eventos) {
            status = aplicar(status, evento.getTipoEvento());
            versaoAtual = evento.getVersao();
        }

        return new Agendamento(agendamentoId, status, versaoAtual);
    }

    private static Status aplicar(Status statusAtual, String tipoEvento) {

        return switch (tipoEvento) {
            case TIPO_CONFIRMADO -> Status.CONFIRMADO;
            case TIPO_CANCELADO -> Status.CANCELADO;
            case TIPO_NAO_COMPARECEU -> Status.NAO_COMPARECEU;
            case TIPO_SINAL_ESTORNADO, TIPO_MULTA_COBRADA -> statusAtual;
            default -> throw new IllegalStateException("Tipo de evento desconhecido: " + tipoEvento);
        };
    }

    public List<EventoAgendamento> cancelar(String ocorridoEm) {

        if (status != Status.CONFIRMADO) {
            throw new IllegalStateException(
                    "Só é possível cancelar um agendamento confirmado. Status atual: " + status);
        }

        return List.of(
                new EventoAgendamento(agendamentoId, versaoAtual + 1, TIPO_CANCELADO, "{}", ocorridoEm),
                new EventoAgendamento(agendamentoId, versaoAtual + 2, TIPO_SINAL_ESTORNADO, "{}", ocorridoEm)
        );
    }

    public List<EventoAgendamento> marcarNaoComparecimento(String ocorridoEm) {

        if (status != Status.CONFIRMADO) {
            throw new IllegalStateException(
                    "Só é possível marcar não comparecimento de um agendamento confirmado. Status atual: " + status);
        }

        return List.of(
                new EventoAgendamento(agendamentoId, versaoAtual + 1, TIPO_NAO_COMPARECEU, "{}", ocorridoEm),
                new EventoAgendamento(agendamentoId, versaoAtual + 2, TIPO_MULTA_COBRADA, "{}", ocorridoEm)
        );
    }

    public String getAgendamentoId() {
        return agendamentoId;
    }

    public Status getStatus() {
        return status;
    }

    public int getVersaoAtual() {
        return versaoAtual;
    }
}
```

- [ ] **Step 5: Rodar e confirmar que passa**

Run: `cd servico-agendamentos && ./mvnw test -Dtest=AgendamentoTest`
Expected: PASS (4 testes)

- [ ] **Step 6: Commit**

```bash
git add servico-agendamentos/src/main/java/br/pucminas/aed/equipe07/agendamentos/domain/EventoAgendamento.java \
        servico-agendamentos/src/main/java/br/pucminas/aed/equipe07/agendamentos/domain/Agendamento.java \
        servico-agendamentos/src/test/java/br/pucminas/aed/equipe07/agendamentos/domain/AgendamentoTest.java
git commit -m "feat: adiciona o agregado Agendamento com fold de eventos"
```

---

### Task 4: O Event Store — `AgendamentoEventStore`

**Files:**
- Create: `servico-agendamentos/src/main/java/br/pucminas/aed/equipe07/agendamentos/service/ConflitoDeVersaoException.java`
- Create: `servico-agendamentos/src/main/java/br/pucminas/aed/equipe07/agendamentos/service/AgendamentoEventStore.java`
- Test: `servico-agendamentos/src/test/java/br/pucminas/aed/equipe07/agendamentos/service/AgendamentoEventStoreTest.java`

- [ ] **Step 1: Escrever o teste que ainda vai falhar**

```java
package br.pucminas.aed.equipe07.agendamentos.service;

import br.pucminas.aed.equipe07.agendamentos.domain.Agendamento;
import br.pucminas.aed.equipe07.agendamentos.domain.EventoAgendamento;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest
class AgendamentoEventStoreTest {

    @Autowired
    private JdbcTemplate banco;

    @Autowired
    private AgendamentoEventStore eventStore;

    @BeforeEach
    void prepararBanco() {

        banco.execute("""
                CREATE TABLE IF NOT EXISTS agendamento_eventos (
                    agendamento_id VARCHAR(100) NOT NULL,
                    versao INTEGER NOT NULL,
                    tipo_evento VARCHAR(60) NOT NULL,
                    payload TEXT NOT NULL,
                    ocorrido_em VARCHAR(40) NOT NULL,
                    gravado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (agendamento_id, versao)
                )
                """);

        banco.update("DELETE FROM agendamento_eventos");
    }

    @Test
    void deveGravarECarregarStreamNaOrdemDeVersao() {

        eventStore.gravar(List.of(
                new EventoAgendamento("AG-600", 1, Agendamento.TIPO_CONFIRMADO, "{}", "2026-09-01T10:00:00-03:00")
        ));

        eventStore.gravar(List.of(
                new EventoAgendamento("AG-600", 2, Agendamento.TIPO_CANCELADO, "{}", "2026-09-02T10:00:00-03:00"),
                new EventoAgendamento("AG-600", 3, Agendamento.TIPO_SINAL_ESTORNADO, "{}", "2026-09-02T10:00:00-03:00")
        ));

        List<EventoAgendamento> stream = eventStore.carregarStream("AG-600");

        assertEquals(3, stream.size());
        assertEquals(1, stream.get(0).getVersao());
        assertEquals(2, stream.get(1).getVersao());
        assertEquals(3, stream.get(2).getVersao());
        assertEquals(Agendamento.TIPO_CONFIRMADO, stream.get(0).getTipoEvento());
    }

    @Test
    void deveLancarConflitoAoGravarVersaoJaExistente() {

        eventStore.gravar(List.of(
                new EventoAgendamento("AG-601", 1, Agendamento.TIPO_CONFIRMADO, "{}", "2026-09-01T10:00:00-03:00")
        ));

        assertThrows(ConflitoDeVersaoException.class, () ->
                eventStore.gravar(List.of(
                        new EventoAgendamento("AG-601", 1, Agendamento.TIPO_CANCELADO, "{}", "2026-09-01T11:00:00-03:00")
                ))
        );
    }
}
```

- [ ] **Step 2: Rodar e confirmar que falha**

Run: `cd servico-agendamentos && ./mvnw test -Dtest=AgendamentoEventStoreTest`
Expected: FAIL — compile error, `AgendamentoEventStore` e `ConflitoDeVersaoException` ainda não existem.

- [ ] **Step 3: Criar `ConflitoDeVersaoException`**

```java
package br.pucminas.aed.equipe07.agendamentos.service;

public class ConflitoDeVersaoException extends RuntimeException {

    public ConflitoDeVersaoException(String mensagem, Throwable causa) {
        super(mensagem, causa);
    }
}
```

- [ ] **Step 4: Criar `AgendamentoEventStore`**

```java
package br.pucminas.aed.equipe07.agendamentos.service;

import br.pucminas.aed.equipe07.agendamentos.domain.EventoAgendamento;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Repository
public class AgendamentoEventStore {

    private final JdbcTemplate banco;

    public AgendamentoEventStore(JdbcTemplate banco) {
        this.banco = banco;
    }

    public List<EventoAgendamento> carregarStream(String agendamentoId) {

        return banco.query(
                """
                SELECT agendamento_id, versao, tipo_evento, payload, ocorrido_em
                FROM agendamento_eventos
                WHERE agendamento_id = ?
                ORDER BY versao
                """,
                mapeador(),
                agendamentoId
        );
    }

    public List<EventoAgendamento> carregarTodosOsEventos() {

        return banco.query(
                """
                SELECT agendamento_id, versao, tipo_evento, payload, ocorrido_em
                FROM agendamento_eventos
                ORDER BY agendamento_id, versao
                """,
                mapeador()
        );
    }

    @Transactional
    public void gravar(List<EventoAgendamento> eventos) {

        try {
            for (EventoAgendamento evento : eventos) {
                banco.update(
                        """
                        INSERT INTO agendamento_eventos (
                            agendamento_id, versao, tipo_evento, payload, ocorrido_em
                        )
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        evento.getAgendamentoId(),
                        evento.getVersao(),
                        evento.getTipoEvento(),
                        evento.getPayload(),
                        evento.getOcorridoEm()
                );
            }
        } catch (DataIntegrityViolationException e) {
            throw new ConflitoDeVersaoException(
                    "Conflito de versão ao gravar eventos do agendamento", e);
        }
    }

    private RowMapper<EventoAgendamento> mapeador() {
        return (rs, rowNum) -> new EventoAgendamento(
                rs.getString("agendamento_id"),
                rs.getInt("versao"),
                rs.getString("tipo_evento"),
                rs.getString("payload"),
                rs.getString("ocorrido_em")
        );
    }
}
```

`carregarTodosOsEventos()` não é usado ainda nesta task — vai ser usado pelo replay da Parte 2. Incluído aqui porque pertence naturalmente ao Event Store, não à projeção.

- [ ] **Step 5: Rodar e confirmar que passa**

Run: `cd servico-agendamentos && ./mvnw test -Dtest=AgendamentoEventStoreTest`
Expected: PASS (2 testes)

- [ ] **Step 6: Commit**

```bash
git add servico-agendamentos/src/main/java/br/pucminas/aed/equipe07/agendamentos/service/ConflitoDeVersaoException.java \
        servico-agendamentos/src/main/java/br/pucminas/aed/equipe07/agendamentos/service/AgendamentoEventStore.java \
        servico-agendamentos/src/test/java/br/pucminas/aed/equipe07/agendamentos/service/AgendamentoEventStoreTest.java
git commit -m "feat: adiciona o event store do agregado Agendamento"
```

---

### Task 5: `AgendamentoService` passa a gravar o evento de confirmação (v1 do stream)

**Files:**
- Modify: `servico-agendamentos/src/main/java/br/pucminas/aed/equipe07/agendamentos/service/AgendamentoService.java`
- Modify: `servico-agendamentos/src/test/java/br/pucminas/aed/equipe07/agendamentos/service/AgendamentoServiceTest.java`

Este é o ponto que a spec identificou como faltante: sem isso, nenhum agendamento teria uma versão 1 no event store, e cancelar/marcar não comparecimento nunca teria o que carregar.

- [ ] **Step 1: Atualizar o teste existente pra também verificar a gravação no event store**

Substituir o conteúdo de `AgendamentoServiceTest.java` por:

```java
package br.pucminas.aed.equipe07.agendamentos.service;

import br.pucminas.aed.equipe07.agendamentos.domain.Agendamento;
import br.pucminas.aed.equipe07.agendamentos.domain.AgendamentoConfirmadoEvent;
import br.pucminas.aed.equipe07.agendamentos.domain.EventoAgendamento;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AgendamentoServiceTest {

    @Test
    void devePublicarAgendamentoConfirmadoComCloudEventsEChaveDoAgendamento() {

        KafkaTemplate<String, AgendamentoConfirmadoEvent> clienteDoBroker =
                mock(KafkaTemplate.class);

        when(clienteDoBroker.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        AgendamentoEventStore eventStore = mock(AgendamentoEventStore.class);
        JsonMapper jsonMapper = JsonMapper.builder().build();

        AgendamentoService service =
                new AgendamentoService(clienteDoBroker, eventStore, jsonMapper);

        AgendamentoConfirmadoEvent evento =
                new AgendamentoConfirmadoEvent(
                        "EVT-001",
                        "AG-001",
                        "PROF-018",
                        "SERV-004",
                        "2026-08-20T14:00:00-03:00",
                        "PADRAO",
                        "2026-08-16T12:30:00-03:00"
                );

        service.publicarConfirmacao(evento);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, AgendamentoConfirmadoEvent>> captor =
                ArgumentCaptor.forClass(ProducerRecord.class);

        verify(clienteDoBroker).send(captor.capture());

        ProducerRecord<String, AgendamentoConfirmadoEvent> registro =
                captor.getValue();

        assertEquals("salao.agendamento-confirmado", registro.topic());
        assertEquals("AG-001", registro.key());
        assertEquals(evento, registro.value());
        assertEquals("1.0", valorDoHeader(registro, "ce_specversion"));
        assertEquals("EVT-001", valorDoHeader(registro, "ce_id"));
        assertEquals("/salao/servico-agendamentos", valorDoHeader(registro, "ce_source"));
        assertEquals("salao.agendamento.confirmado.v1", valorDoHeader(registro, "ce_type"));
        assertEquals("2026-08-16T12:30:00-03:00", valorDoHeader(registro, "ce_time"));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<EventoAgendamento>> captorEventos =
                ArgumentCaptor.forClass(List.class);

        verify(eventStore).gravar(captorEventos.capture());

        List<EventoAgendamento> eventosGravados = captorEventos.getValue();

        assertEquals(1, eventosGravados.size());
        assertEquals("AG-001", eventosGravados.get(0).getAgendamentoId());
        assertEquals(1, eventosGravados.get(0).getVersao());
        assertEquals(Agendamento.TIPO_CONFIRMADO, eventosGravados.get(0).getTipoEvento());
    }

    private String valorDoHeader(
            ProducerRecord<String, AgendamentoConfirmadoEvent> registro,
            String nome) {

        Header header = registro.headers().lastHeader(nome);

        assertNotNull(header);

        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
```

- [ ] **Step 2: Rodar e confirmar que falha**

Run: `cd servico-agendamentos && ./mvnw test -Dtest=AgendamentoServiceTest`
Expected: FAIL — compile error, o construtor de `AgendamentoService` ainda tem só 1 parâmetro.

- [ ] **Step 3: Modificar `AgendamentoService`**

```java
package br.pucminas.aed.equipe07.agendamentos.service;

import br.pucminas.aed.equipe07.agendamentos.domain.Agendamento;
import br.pucminas.aed.equipe07.agendamentos.domain.AgendamentoConfirmadoEvent;
import br.pucminas.aed.equipe07.agendamentos.domain.EventoAgendamento;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
public class AgendamentoService {

    private static final String TOPICO = "salao.agendamento-confirmado";

    private final KafkaTemplate<String, AgendamentoConfirmadoEvent> clienteDoBroker;
    private final AgendamentoEventStore eventStore;
    private final JsonMapper jsonMapper;

    public AgendamentoService(
            KafkaTemplate<String, AgendamentoConfirmadoEvent> clienteDoBroker,
            AgendamentoEventStore eventStore,
            JsonMapper jsonMapper) {

        this.clienteDoBroker = clienteDoBroker;
        this.eventStore = eventStore;
        this.jsonMapper = jsonMapper;
    }

    public CompletableFuture<SendResult<String, AgendamentoConfirmadoEvent>>
    publicarConfirmacao(AgendamentoConfirmadoEvent evento) {

        eventStore.gravar(List.of(
                new EventoAgendamento(
                        evento.getAgendamentoId(),
                        1,
                        Agendamento.TIPO_CONFIRMADO,
                        jsonMapper.writeValueAsString(evento),
                        evento.getOcorridoEm()
                )
        ));

        RecordHeaders headers = new RecordHeaders();

        adicionarHeader(headers, "ce_specversion", "1.0");
        adicionarHeader(headers, "ce_id", evento.getEventoId());
        adicionarHeader(headers, "ce_source", "/salao/servico-agendamentos");
        adicionarHeader(headers, "ce_type", "salao.agendamento.confirmado.v1");
        adicionarHeader(headers, "ce_time", evento.getOcorridoEm());

        ProducerRecord<String, AgendamentoConfirmadoEvent> registro =
                new ProducerRecord<>(
                        TOPICO,
                        null,
                        evento.getAgendamentoId(),
                        evento,
                        headers
                );

        CompletableFuture<SendResult<String, AgendamentoConfirmadoEvent>> resultado =
                clienteDoBroker.send(registro);

        resultado.whenComplete(this::tratarResultadoDaPublicacao);

        return resultado;
    }

    private void adicionarHeader(RecordHeaders headers, String nome, String valor) {
        headers.add(nome, valor.getBytes(StandardCharsets.UTF_8));
    }

    private void tratarResultadoDaPublicacao(
            SendResult<String, AgendamentoConfirmadoEvent> resultado,
            Throwable erro) {

        if (erro != null) {
            System.err.println(
                    "Falha ao publicar AgendamentoConfirmadoEvent: " + erro.getMessage());
        }
    }
}
```

- [ ] **Step 4: Rodar e confirmar que passa**

Run: `cd servico-agendamentos && ./mvnw test -Dtest=AgendamentoServiceTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add servico-agendamentos/src/main/java/br/pucminas/aed/equipe07/agendamentos/service/AgendamentoService.java \
        servico-agendamentos/src/test/java/br/pucminas/aed/equipe07/agendamentos/service/AgendamentoServiceTest.java
git commit -m "feat: AgendamentoService grava o evento de confirmacao no event store"
```

---

### Task 6: `AgendamentoCicloDeVidaService` — cancelar e marcar não comparecimento

**Files:**
- Create: `servico-agendamentos/src/main/java/br/pucminas/aed/equipe07/agendamentos/service/AgendamentoCicloDeVidaService.java`
- Test: `servico-agendamentos/src/test/java/br/pucminas/aed/equipe07/agendamentos/service/AgendamentoCicloDeVidaServiceTest.java`

Sem projetor ainda — isso entra na Parte 2, Task 8.

- [ ] **Step 1: Escrever o teste que ainda vai falhar**

```java
package br.pucminas.aed.equipe07.agendamentos.service;

import br.pucminas.aed.equipe07.agendamentos.domain.Agendamento;
import br.pucminas.aed.equipe07.agendamentos.domain.EventoAgendamento;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest
class AgendamentoCicloDeVidaServiceTest {

    @Autowired
    private JdbcTemplate banco;

    @Autowired
    private AgendamentoEventStore eventStore;

    @Autowired
    private AgendamentoCicloDeVidaService cicloDeVidaService;

    @BeforeEach
    void prepararBanco() {

        banco.execute("""
                CREATE TABLE IF NOT EXISTS agendamento_eventos (
                    agendamento_id VARCHAR(100) NOT NULL,
                    versao INTEGER NOT NULL,
                    tipo_evento VARCHAR(60) NOT NULL,
                    payload TEXT NOT NULL,
                    ocorrido_em VARCHAR(40) NOT NULL,
                    gravado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (agendamento_id, versao)
                )
                """);

        banco.update("DELETE FROM agendamento_eventos");
    }

    @Test
    void deveCancelarUmAgendamentoConfirmadoEGravarOsDoisEventos() {

        eventStore.gravar(List.of(
                new EventoAgendamento("AG-800", 1, Agendamento.TIPO_CONFIRMADO, "{}", "2026-09-01T10:00:00-03:00")
        ));

        cicloDeVidaService.cancelar("AG-800");

        List<EventoAgendamento> stream = eventStore.carregarStream("AG-800");

        assertEquals(3, stream.size());
        assertEquals(Agendamento.TIPO_CANCELADO, stream.get(1).getTipoEvento());
        assertEquals(Agendamento.TIPO_SINAL_ESTORNADO, stream.get(2).getTipoEvento());
    }

    @Test
    void deveMarcarNaoComparecimentoEGravarOsDoisEventos() {

        eventStore.gravar(List.of(
                new EventoAgendamento("AG-801", 1, Agendamento.TIPO_CONFIRMADO, "{}", "2026-09-01T10:00:00-03:00")
        ));

        cicloDeVidaService.marcarNaoComparecimento("AG-801");

        List<EventoAgendamento> stream = eventStore.carregarStream("AG-801");

        assertEquals(3, stream.size());
        assertEquals(Agendamento.TIPO_NAO_COMPARECEU, stream.get(1).getTipoEvento());
        assertEquals(Agendamento.TIPO_MULTA_COBRADA, stream.get(2).getTipoEvento());
    }

    @Test
    void naoDeveCancelarUmAgendamentoQueJaFoiCancelado() {

        eventStore.gravar(List.of(
                new EventoAgendamento("AG-802", 1, Agendamento.TIPO_CONFIRMADO, "{}", "2026-09-01T10:00:00-03:00")
        ));

        cicloDeVidaService.cancelar("AG-802");

        assertThrows(IllegalStateException.class, () -> cicloDeVidaService.cancelar("AG-802"));
    }
}
```

- [ ] **Step 2: Rodar e confirmar que falha**

Run: `cd servico-agendamentos && ./mvnw test -Dtest=AgendamentoCicloDeVidaServiceTest`
Expected: FAIL — compile error, `AgendamentoCicloDeVidaService` ainda não existe.

- [ ] **Step 3: Criar `AgendamentoCicloDeVidaService`**

```java
package br.pucminas.aed.equipe07.agendamentos.service;

import br.pucminas.aed.equipe07.agendamentos.domain.Agendamento;
import br.pucminas.aed.equipe07.agendamentos.domain.EventoAgendamento;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.List;

@Service
public class AgendamentoCicloDeVidaService {

    private final AgendamentoEventStore eventStore;

    public AgendamentoCicloDeVidaService(AgendamentoEventStore eventStore) {
        this.eventStore = eventStore;
    }

    public void cancelar(String agendamentoId) {

        Agendamento agendamento = carregarAgregado(agendamentoId);

        List<EventoAgendamento> novosEventos =
                agendamento.cancelar(OffsetDateTime.now().toString());

        eventStore.gravar(novosEventos);
    }

    public void marcarNaoComparecimento(String agendamentoId) {

        Agendamento agendamento = carregarAgregado(agendamentoId);

        List<EventoAgendamento> novosEventos =
                agendamento.marcarNaoComparecimento(OffsetDateTime.now().toString());

        eventStore.gravar(novosEventos);
    }

    private Agendamento carregarAgregado(String agendamentoId) {
        return Agendamento.reconstruir(eventStore.carregarStream(agendamentoId));
    }
}
```

- [ ] **Step 4: Rodar e confirmar que passa**

Run: `cd servico-agendamentos && ./mvnw test -Dtest=AgendamentoCicloDeVidaServiceTest`
Expected: PASS (3 testes)

- [ ] **Step 5: Commit**

```bash
git add servico-agendamentos/src/main/java/br/pucminas/aed/equipe07/agendamentos/service/AgendamentoCicloDeVidaService.java \
        servico-agendamentos/src/test/java/br/pucminas/aed/equipe07/agendamentos/service/AgendamentoCicloDeVidaServiceTest.java
git commit -m "feat: adiciona cancelamento e nao-comparecimento ao ciclo de vida do agendamento"
```

---

### Task 7: Endpoints HTTP — cancelar e marcar não comparecimento

**Files:**
- Modify: `servico-agendamentos/src/main/java/br/pucminas/aed/equipe07/agendamentos/controller/AgendamentoController.java`
- Modify: `servico-agendamentos/src/test/java/br/pucminas/aed/equipe07/agendamentos/controller/AgendamentoControllerTest.java`

- [ ] **Step 1: Atualizar o teste existente e adicionar os dois novos casos**

Substituir o conteúdo de `AgendamentoControllerTest.java` por:

```java
package br.pucminas.aed.equipe07.agendamentos.controller;

import br.pucminas.aed.equipe07.agendamentos.domain.AgendamentoConfirmadoEvent;
import br.pucminas.aed.equipe07.agendamentos.service.AgendamentoCicloDeVidaService;
import br.pucminas.aed.equipe07.agendamentos.service.AgendamentoService;
import br.pucminas.aed.equipe07.agendamentos.service.ConflitoDeVersaoException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class AgendamentoControllerTest {

    @Test
    void deveResponderAcceptedAoDispararConfirmacao() {

        AgendamentoService service = mock(AgendamentoService.class);
        AgendamentoCicloDeVidaService cicloDeVidaService = mock(AgendamentoCicloDeVidaService.class);

        AgendamentoController controller =
                new AgendamentoController(service, cicloDeVidaService);

        AgendamentoConfirmadoEvent evento =
                new AgendamentoConfirmadoEvent(
                        "EVT-001", "AG-001", "PROF-018", "SERV-004",
                        "2026-08-20T14:00:00-03:00", "PADRAO", "2026-08-16T12:30:00-03:00"
                );

        ResponseEntity<Void> resposta = controller.publicarConfirmacao(evento);

        assertEquals(HttpStatus.ACCEPTED, resposta.getStatusCode());
        verify(service).publicarConfirmacao(evento);
    }

    @Test
    void deveReceberConfirmacaoPorHttpEResponderAccepted() throws Exception {

        AgendamentoService service = mock(AgendamentoService.class);
        AgendamentoCicloDeVidaService cicloDeVidaService = mock(AgendamentoCicloDeVidaService.class);

        AgendamentoController controller =
                new AgendamentoController(service, cicloDeVidaService);

        MockMvc mockMvc = MockMvcBuilders.standaloneSetup(controller).build();

        String json = """
                {
                    "eventoId": "EVT-001",
                    "agendamentoId": "AG-001",
                    "profissionalId": "PROF-018",
                    "servicoId": "SERV-004",
                    "inicioEm": "2026-08-20T14:00:00-03:00",
                    "prioridade": "PADRAO",
                    "ocorridoEm": "2026-08-16T12:30:00-03:00"
                }
                """;

        mockMvc.perform(
                        post("/agendamentos/confirmacoes")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(json)
                )
                .andExpect(status().isAccepted());

        ArgumentCaptor<AgendamentoConfirmadoEvent> captor =
                ArgumentCaptor.forClass(AgendamentoConfirmadoEvent.class);

        verify(service).publicarConfirmacao(captor.capture());

        assertEquals("EVT-001", captor.getValue().getEventoId());
        assertEquals("AG-001", captor.getValue().getAgendamentoId());
    }

    @Test
    void deveResponderOkAoCancelarComSucesso() {

        AgendamentoService service = mock(AgendamentoService.class);
        AgendamentoCicloDeVidaService cicloDeVidaService = mock(AgendamentoCicloDeVidaService.class);

        AgendamentoController controller =
                new AgendamentoController(service, cicloDeVidaService);

        ResponseEntity<Void> resposta = controller.cancelar("AG-001");

        assertEquals(HttpStatus.OK, resposta.getStatusCode());
        verify(cicloDeVidaService).cancelar("AG-001");
    }

    @Test
    void deveResponderConflictAoCancelarUmAgendamentoEmEstadoInvalido() {

        AgendamentoService service = mock(AgendamentoService.class);
        AgendamentoCicloDeVidaService cicloDeVidaService = mock(AgendamentoCicloDeVidaService.class);

        doThrow(new IllegalStateException("já cancelado"))
                .when(cicloDeVidaService).cancelar("AG-002");

        AgendamentoController controller =
                new AgendamentoController(service, cicloDeVidaService);

        ResponseEntity<Void> resposta = controller.cancelar("AG-002");

        assertEquals(HttpStatus.CONFLICT, resposta.getStatusCode());
    }

    @Test
    void deveResponderConflictAoCancelarComConflitoDeVersao() {

        AgendamentoService service = mock(AgendamentoService.class);
        AgendamentoCicloDeVidaService cicloDeVidaService = mock(AgendamentoCicloDeVidaService.class);

        doThrow(new ConflitoDeVersaoException("conflito", null))
                .when(cicloDeVidaService).cancelar("AG-003");

        AgendamentoController controller =
                new AgendamentoController(service, cicloDeVidaService);

        ResponseEntity<Void> resposta = controller.cancelar("AG-003");

        assertEquals(HttpStatus.CONFLICT, resposta.getStatusCode());
    }

    @Test
    void deveResponderOkAoMarcarNaoComparecimentoComSucesso() {

        AgendamentoService service = mock(AgendamentoService.class);
        AgendamentoCicloDeVidaService cicloDeVidaService = mock(AgendamentoCicloDeVidaService.class);

        AgendamentoController controller =
                new AgendamentoController(service, cicloDeVidaService);

        ResponseEntity<Void> resposta = controller.marcarNaoComparecimento("AG-004");

        assertEquals(HttpStatus.OK, resposta.getStatusCode());
        verify(cicloDeVidaService).marcarNaoComparecimento("AG-004");
    }
}
```

- [ ] **Step 2: Rodar e confirmar que falha**

Run: `cd servico-agendamentos && ./mvnw test -Dtest=AgendamentoControllerTest`
Expected: FAIL — compile error, o construtor de `AgendamentoController` ainda tem só 1 parâmetro, e os métodos `cancelar`/`marcarNaoComparecimento` ainda não existem.

- [ ] **Step 3: Modificar `AgendamentoController`**

```java
package br.pucminas.aed.equipe07.agendamentos.controller;

import br.pucminas.aed.equipe07.agendamentos.domain.AgendamentoConfirmadoEvent;
import br.pucminas.aed.equipe07.agendamentos.service.AgendamentoCicloDeVidaService;
import br.pucminas.aed.equipe07.agendamentos.service.AgendamentoService;
import br.pucminas.aed.equipe07.agendamentos.service.ConflitoDeVersaoException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/agendamentos")
public class AgendamentoController {

    private final AgendamentoService service;
    private final AgendamentoCicloDeVidaService cicloDeVidaService;

    public AgendamentoController(
            AgendamentoService service,
            AgendamentoCicloDeVidaService cicloDeVidaService) {

        this.service = service;
        this.cicloDeVidaService = cicloDeVidaService;
    }

    @PostMapping("/confirmacoes")
    public ResponseEntity<Void> publicarConfirmacao(
            @RequestBody AgendamentoConfirmadoEvent evento) {

        service.publicarConfirmacao(evento);

        return ResponseEntity.accepted().build();
    }

    @PostMapping("/{agendamentoId}/cancelar")
    public ResponseEntity<Void> cancelar(@PathVariable String agendamentoId) {

        try {
            cicloDeVidaService.cancelar(agendamentoId);
            return ResponseEntity.ok().build();

        } catch (ConflitoDeVersaoException | IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).build();
        }
    }

    @PostMapping("/{agendamentoId}/nao-comparecimento")
    public ResponseEntity<Void> marcarNaoComparecimento(@PathVariable String agendamentoId) {

        try {
            cicloDeVidaService.marcarNaoComparecimento(agendamentoId);
            return ResponseEntity.ok().build();

        } catch (ConflitoDeVersaoException | IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).build();
        }
    }
}
```

- [ ] **Step 4: Rodar e confirmar que passa**

Run: `cd servico-agendamentos && ./mvnw test -Dtest=AgendamentoControllerTest`
Expected: PASS (6 testes)

- [ ] **Step 5: Rodar a suíte inteira do módulo**

Run: `cd servico-agendamentos && ./mvnw test`
Expected: PASS em tudo — este é o fim da Parte 1. Neste ponto, `servico-agendamentos` já confirma, cancela e marca não comparecimento, tudo gravando no event store, com conflito de versão detectado.

- [ ] **Step 6: Commit**

```bash
git add servico-agendamentos/src/main/java/br/pucminas/aed/equipe07/agendamentos/controller/AgendamentoController.java \
        servico-agendamentos/src/test/java/br/pucminas/aed/equipe07/agendamentos/controller/AgendamentoControllerTest.java
git commit -m "feat: adiciona endpoints de cancelamento e nao-comparecimento"
```

---

# PARTE 2 — Projeção CQRS + Testes + Documentação (Tarefas 3, 4 e 5 do enunciado)

### Task 8: A projeção — `ProjetorFaltasEstornos`

**Files:**
- Modify: `docker/postgres-init/init-agendamentos.sql`
- Create: `servico-agendamentos/src/main/java/br/pucminas/aed/equipe07/agendamentos/service/ProjetorFaltasEstornos.java`
- Modify: `servico-agendamentos/src/main/java/br/pucminas/aed/equipe07/agendamentos/service/AgendamentoCicloDeVidaService.java`
- Modify: `servico-agendamentos/src/test/java/br/pucminas/aed/equipe07/agendamentos/service/AgendamentoCicloDeVidaServiceTest.java`

- [ ] **Step 1: Adicionar a tabela da projeção ao script de inicialização**

Em `docker/postgres-init/init-agendamentos.sql`, adicionar ao final:

```sql
CREATE TABLE IF NOT EXISTS relatorio_faltas_estornos (
    data VARCHAR(10) NOT NULL,
    tipo VARCHAR(20) NOT NULL,
    quantidade INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (data, tipo)
);
```

- [ ] **Step 2: Criar `ProjetorFaltasEstornos`**

Sem TDD isolado aqui — o teste que prova este componente é o teste de replay da Task 9, que é mais valioso testando os dois juntos (gravar + reconstruir) do que um teste unitário de `processar` sozinho.

```java
package br.pucminas.aed.equipe07.agendamentos.service;

import br.pucminas.aed.equipe07.agendamentos.domain.Agendamento;
import br.pucminas.aed.equipe07.agendamentos.domain.EventoAgendamento;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ProjetorFaltasEstornos {

    private static final String TIPO_ESTORNO = "ESTORNO";
    private static final String TIPO_MULTA = "MULTA";

    private final JdbcTemplate banco;
    private final AgendamentoEventStore eventStore;

    public ProjetorFaltasEstornos(JdbcTemplate banco, AgendamentoEventStore eventStore) {
        this.banco = banco;
        this.eventStore = eventStore;
    }

    public void processar(List<EventoAgendamento> eventos) {

        for (EventoAgendamento evento : eventos) {

            String tipoProjecao = tipoDaProjecao(evento.getTipoEvento());

            if (tipoProjecao != null) {
                incrementar(dataDoEvento(evento), tipoProjecao);
            }
        }
    }

    public void reconstruirDoZero() {

        banco.update("DELETE FROM relatorio_faltas_estornos");

        processar(eventStore.carregarTodosOsEventos());
    }

    private String tipoDaProjecao(String tipoEvento) {

        if (Agendamento.TIPO_SINAL_ESTORNADO.equals(tipoEvento)) {
            return TIPO_ESTORNO;
        }

        if (Agendamento.TIPO_MULTA_COBRADA.equals(tipoEvento)) {
            return TIPO_MULTA;
        }

        return null;
    }

    private String dataDoEvento(EventoAgendamento evento) {
        return evento.getOcorridoEm().substring(0, 10);
    }

    private void incrementar(String data, String tipo) {

        int linhasAtualizadas = banco.update(
                """
                UPDATE relatorio_faltas_estornos
                SET quantidade = quantidade + 1
                WHERE data = ? AND tipo = ?
                """,
                data, tipo
        );

        if (linhasAtualizadas == 0) {
            banco.update(
                    """
                    INSERT INTO relatorio_faltas_estornos (data, tipo, quantidade)
                    VALUES (?, ?, 1)
                    """,
                    data, tipo
            );
        }
    }
}
```

Nota sobre `dataDoEvento`: assume que `ocorridoEm` está no formato ISO-8601 (`"2026-09-01T10:00:00-03:00"`), então os 10 primeiros caracteres são sempre `"2026-09-01"`. Esse formato já é garantido em todo o resto do projeto (mesma convenção de `docs/contrato.md`).

- [ ] **Step 3: Ligar o projetor ao ciclo de vida do agendamento**

Modificar `AgendamentoCicloDeVidaService.java`:

```java
package br.pucminas.aed.equipe07.agendamentos.service;

import br.pucminas.aed.equipe07.agendamentos.domain.Agendamento;
import br.pucminas.aed.equipe07.agendamentos.domain.EventoAgendamento;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.List;

@Service
public class AgendamentoCicloDeVidaService {

    private final AgendamentoEventStore eventStore;
    private final ProjetorFaltasEstornos projetor;

    public AgendamentoCicloDeVidaService(
            AgendamentoEventStore eventStore,
            ProjetorFaltasEstornos projetor) {

        this.eventStore = eventStore;
        this.projetor = projetor;
    }

    public void cancelar(String agendamentoId) {

        Agendamento agendamento = carregarAgregado(agendamentoId);

        List<EventoAgendamento> novosEventos =
                agendamento.cancelar(OffsetDateTime.now().toString());

        eventStore.gravar(novosEventos);
        projetor.processar(novosEventos);
    }

    public void marcarNaoComparecimento(String agendamentoId) {

        Agendamento agendamento = carregarAgregado(agendamentoId);

        List<EventoAgendamento> novosEventos =
                agendamento.marcarNaoComparecimento(OffsetDateTime.now().toString());

        eventStore.gravar(novosEventos);
        projetor.processar(novosEventos);
    }

    private Agendamento carregarAgregado(String agendamentoId) {
        return Agendamento.reconstruir(eventStore.carregarStream(agendamentoId));
    }
}
```

A projeção é atualizada **depois** de `eventStore.gravar(...)` já ter retornado (ou seja, depois do commit da transação de escrita) — não dentro da mesma transação. Essa ordem é proposital: é o que cria a janela real de consistência eventual que `docs/entregas/aula-05.md` (Task 10) vai documentar.

- [ ] **Step 4: Atualizar `AgendamentoCicloDeVidaServiceTest` pro novo construtor**

Em `AgendamentoCicloDeVidaServiceTest.java`, adicionar o import e o autowire de `ProjetorFaltasEstornos`, e criar a tabela `relatorio_faltas_estornos` no `@BeforeEach` (senão o `INSERT`/`UPDATE` do projetor falha por tabela inexistente):

```java
    @Autowired
    private ProjetorFaltasEstornos projetor;

    @BeforeEach
    void prepararBanco() {

        banco.execute("""
                CREATE TABLE IF NOT EXISTS agendamento_eventos (
                    agendamento_id VARCHAR(100) NOT NULL,
                    versao INTEGER NOT NULL,
                    tipo_evento VARCHAR(60) NOT NULL,
                    payload TEXT NOT NULL,
                    ocorrido_em VARCHAR(40) NOT NULL,
                    gravado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (agendamento_id, versao)
                )
                """);

        banco.execute("""
                CREATE TABLE IF NOT EXISTS relatorio_faltas_estornos (
                    data VARCHAR(10) NOT NULL,
                    tipo VARCHAR(20) NOT NULL,
                    quantidade INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (data, tipo)
                )
                """);

        banco.update("DELETE FROM agendamento_eventos");
        banco.update("DELETE FROM relatorio_faltas_estornos");
    }
```

(O `@Autowired private ProjetorFaltasEstornos projetor;` fica sem uso direto nos testes existentes — está ali só porque `AgendamentoCicloDeVidaServiceTest` sobe o contexto Spring completo, e sem a tabela `relatorio_faltas_estornos` os testes de `cancelar`/`marcarNaoComparecimento` iriam falhar ao tentar `INSERT` nela.)

- [ ] **Step 5: Rodar e confirmar que tudo passa**

Run: `cd servico-agendamentos && ./mvnw test -Dtest=AgendamentoCicloDeVidaServiceTest`
Expected: PASS (3 testes, os mesmos de antes — só passaram a também atualizar a projeção)

- [ ] **Step 6: Commit**

```bash
git add docker/postgres-init/init-agendamentos.sql \
        servico-agendamentos/src/main/java/br/pucminas/aed/equipe07/agendamentos/service/ProjetorFaltasEstornos.java \
        servico-agendamentos/src/main/java/br/pucminas/aed/equipe07/agendamentos/service/AgendamentoCicloDeVidaService.java \
        servico-agendamentos/src/test/java/br/pucminas/aed/equipe07/agendamentos/service/AgendamentoCicloDeVidaServiceTest.java
git commit -m "feat: adiciona a projecao de faltas e estornos"
```

---

### Task 9: O teste de replay — o critério mais importante da entrega

**Files:**
- Test: `servico-agendamentos/src/test/java/br/pucminas/aed/equipe07/agendamentos/service/ProjetorFaltasEstornosReplayTest.java`

- [ ] **Step 1: Escrever o teste**

```java
package br.pucminas.aed.equipe07.agendamentos.service;

import br.pucminas.aed.equipe07.agendamentos.domain.Agendamento;
import br.pucminas.aed.equipe07.agendamentos.domain.EventoAgendamento;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
class ProjetorFaltasEstornosReplayTest {

    @Autowired
    private JdbcTemplate banco;

    @Autowired
    private AgendamentoEventStore eventStore;

    @Autowired
    private ProjetorFaltasEstornos projetor;

    @BeforeEach
    void prepararBanco() {

        banco.execute("""
                CREATE TABLE IF NOT EXISTS agendamento_eventos (
                    agendamento_id VARCHAR(100) NOT NULL,
                    versao INTEGER NOT NULL,
                    tipo_evento VARCHAR(60) NOT NULL,
                    payload TEXT NOT NULL,
                    ocorrido_em VARCHAR(40) NOT NULL,
                    gravado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (agendamento_id, versao)
                )
                """);

        banco.execute("""
                CREATE TABLE IF NOT EXISTS relatorio_faltas_estornos (
                    data VARCHAR(10) NOT NULL,
                    tipo VARCHAR(20) NOT NULL,
                    quantidade INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (data, tipo)
                )
                """);

        banco.update("DELETE FROM agendamento_eventos");
        banco.update("DELETE FROM relatorio_faltas_estornos");
    }

    @Test
    void deveReconstruirAProjecaoIdenticaAposApagarTudoEReprocessarOLog() {

        eventStore.gravar(List.of(
                new EventoAgendamento("AG-501", 1, Agendamento.TIPO_CONFIRMADO, "{}", "2026-09-01T10:00:00-03:00")
        ));

        Agendamento agendamento1 = Agendamento.reconstruir(eventStore.carregarStream("AG-501"));
        List<EventoAgendamento> cancelamento = agendamento1.cancelar("2026-09-02T09:00:00-03:00");
        eventStore.gravar(cancelamento);
        projetor.processar(cancelamento);

        eventStore.gravar(List.of(
                new EventoAgendamento("AG-502", 1, Agendamento.TIPO_CONFIRMADO, "{}", "2026-09-01T11:00:00-03:00")
        ));

        Agendamento agendamento2 = Agendamento.reconstruir(eventStore.carregarStream("AG-502"));
        List<EventoAgendamento> naoComparecimento = agendamento2.marcarNaoComparecimento("2026-09-01T18:00:00-03:00");
        eventStore.gravar(naoComparecimento);
        projetor.processar(naoComparecimento);

        Integer estornosAntes = contar("2026-09-02", "ESTORNO");
        Integer multasAntes = contar("2026-09-01", "MULTA");

        assertEquals(1, estornosAntes);
        assertEquals(1, multasAntes);

        banco.update("DELETE FROM relatorio_faltas_estornos");

        projetor.reconstruirDoZero();

        Integer estornosDepois = contar("2026-09-02", "ESTORNO");
        Integer multasDepois = contar("2026-09-01", "MULTA");

        assertEquals(estornosAntes, estornosDepois);
        assertEquals(multasAntes, multasDepois);
    }

    private Integer contar(String data, String tipo) {
        return banco.queryForObject(
                "SELECT quantidade FROM relatorio_faltas_estornos WHERE data = ? AND tipo = ?",
                Integer.class, data, tipo
        );
    }
}
```

- [ ] **Step 2: Rodar e confirmar que passa**

Run: `cd servico-agendamentos && ./mvnw test -Dtest=ProjetorFaltasEstornosReplayTest`
Expected: PASS — este teste prova exatamente o que a Tarefa 3 exige: apaga a projeção inteira, reconstrói via replay do event store, confirma que os números batem com os de antes.

- [ ] **Step 3: Rodar a suíte inteira do módulo**

Run: `cd servico-agendamentos && ./mvnw test`
Expected: PASS em tudo.

- [ ] **Step 4: Commit**

```bash
git add servico-agendamentos/src/test/java/br/pucminas/aed/equipe07/agendamentos/service/ProjetorFaltasEstornosReplayTest.java
git commit -m "test: prova que a projecao de faltas e estornos e reconstruivel via replay"
```

---

### Task 10: `docs/entregas/aula-05.md`

**Files:**
- Create: `docs/entregas/aula-05.md`

- [ ] **Step 1: Escrever o documento**

```markdown
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
```

- [ ] **Step 2: Commit**

```bash
git add docs/entregas/aula-05.md
git commit -m "docs: adiciona a entrega da aula 05"
```

---

### Task 11: `docs/IA.md`

**Files:**
- Modify: `docs/IA.md`

- [ ] **Step 1: Ler o arquivo atual pra confirmar o próximo número de interação**

Rodar `grep -n "^### Interação" docs/IA.md` antes de escrever — no momento em que este plano foi escrito, o arquivo já ia até "Interação 6" (sobre a Aula 04), então esta entrada é "Interação 7". Confirme o número real lendo o arquivo, não assuma o número abaixo se o arquivo tiver mudado de novo.

- [ ] **Step 2: Adicionar a entrada, incluindo a recusa pedida pela própria tarefa**

Adicionar ao final de `docs/IA.md`:

```markdown
---

### Interação 7 — Event Sourcing e CQRS no agregado Agendamento

**O que foi pedido:** aplicar Event Sourcing a um agregado do domínio com caminho de
exceção (compensação/auditoria), com um Event Store append-only/ordenado/versionado
por stream, e uma projeção CQRS reconstruível via replay.

**O que a ferramenta sugeriu:** o agregado `Agendamento`, cobrindo o caminho
Confirmado → (Cancelado + SinalEstornado) ou (NaoCompareceu + MultaCobrada) — já
antecipado no ADR-002 como o caminho de exceção do domínio, mas nunca implementado.
Event Store como tabela `agendamento_eventos` com `PRIMARY KEY (agendamento_id, versao)`
detectando conflito de concorrência via `DataIntegrityViolationException`; projeção
`relatorio_faltas_estornos`, atualizada depois (não dentro) da transação de escrita,
de propósito, para expor a janela real de consistência eventual.

**O que foi aceito:** o agregado escolhido, a separação em dois eventos por caminho de
exceção (em vez de um evento composto — mostra melhor a regra de versão por stream), a
tabela de projeção reaproveitando o padrão de upsert já usado em `servico-ocupacao`, e
não implementar snapshot (volume pequeno demais para justificar).

**O que foi recusado, e por quê — exercício deliberado de contraditório pedido pela
própria tarefa:**

- **Aplicar Event Sourcing também à projeção de ocupação (`OcupacaoService`) e ao
  agregador por janela**, por "consistência" com o resto do sistema: recusado. Nenhum
  dos dois tem caminho de exceção com compensação — são leituras derivadas de um fluxo
  de eventos que já existe no Kafka, sem necessidade de reconstruir estado por fold.
  Aplicar Event Sourcing ali seria decisão por moda, não por agregado, e o próprio
  enunciado da aula pede o contrário: heterogeneidade (alguns agregados event-sourced,
  outros não) é acerto de escopo, não dívida técnica.
- **Corrigir um evento já gravado com `UPDATE` direto na tabela `agendamento_eventos`**,
  levantada como forma "mais simples" de consertar um dado errado depois do fato:
  recusada. Um Event Store append-only não admite `UPDATE`/`DELETE` — a correção certa
  é gravar um novo evento de correção (ex.: um evento futuro que anota que a versão N
  estava errada), preservando a sequência real dos fatos, inclusive o erro e sua
  correção. Fazer `UPDATE` reescreveria a história e quebraria a garantia nº 1 (append-only)
  que a própria entrega pede para respeitar.
- **Atualizar a projeção dentro da mesma transação do event store**, pra "garantir
  consistência forte": recusada. Isso eliminaria a janela de consistência eventual que a
  Tarefa 4 pede para documentar — faria a entrega tecnicamente mais simples, mas
  esconderia exatamente o comportamento (defasagem entre escrita e leitura) que a aula
  quer que a equipe saiba nomear e justificar.
```

- [ ] **Step 3: Commit**

```bash
git add docs/IA.md
git commit -m "docs: registra decisoes de IA da aula 05, incluindo recusas do contraditorio"
```
