## Aula 02

### Interação 1 — Consumer Kafka no serviço de ocupação

**O que foi pedido:** criar um listener que consome eventos do tópico
`salao.agendamento-confirmado` e processa a ocupação do profissional.

**O que a ferramenta sugeriu:**

```java
@Component
public class AgendamentoConfirmadoListener {

    private final OcupacaoService service;

    public AgendamentoConfirmadoListener(OcupacaoService service) {
        this.service = service;
    }

    @KafkaListener(topics = "salao.agendamento-confirmado", groupId = "ocupacao-service")
    public void processar(AgendamentoConfirmadoEvent evento,
                         @Header(name = "ce_id") String eventoId) {
        try {
            service.processar(evento);
            System.out.println("Evento processado: " + eventoId);
        } catch (Exception e) {
            System.err.println("Erro ao processar: " + e.getMessage());
        }
    }
}
```

A ferramenta também sugeriu adicionar retry automático com `@Retry` do Spring Retry.

**O que foi aceito:** a estrutura básica do listener com `@KafkaListener`, a injeção do
`OcupacaoService` por construtor, e a captura do header `ce_id` para rastreabilidade do
evento no log.

**O que foi recusado, e por quê:**

- **Retry automático com `@Retry`**: o Spring Kafka já tem mecanismo nativo de retry via
  `maxAttempts`/`backoffDelay`. Adicionar `@Retry` criaria duas estratégias de retry
  concorrendo entre si (uma no nível do listener, outra síncrona no método). Além disso,
  em arquitetura orientada a eventos, falha recorrente deve fluir para uma Dead Letter
  Topic, não ser retentada indefinidamente de forma síncrona.
- **Try-catch genérico com `System.err`**: logging direto no console é inadequado fora de
  ambiente de demonstração. A sugestão foi trocada por logging estruturado (SLF4J),
  distinguindo erro de negócio de falha técnica — um erro de header não deve ser tratado
  da mesma forma que uma falha de banco.

---

### Interação 2 — Idempotência do consumidor

**O que foi pedido:** garantir que o mesmo evento não seja processado duas vezes, mesmo se
chegar duplicado do Kafka.

**O que a ferramenta sugeriu:**

```java
@Service
public class OcupacaoService {

    private final OcupacaoRepository repository;

    @Transactional
    public void processar(AgendamentoConfirmadoEvent evento) {
        if (repository.eventoJaProcessado(evento.getEventoId())) {
            return;
        }
        repository.registrarEventoProcessado(evento.getEventoId());
        repository.registrarOcupacao(
            evento.getAgendamentoId(),
            evento.getProfissionalId(),
            evento.getInicioEm()
        );
    }
}
```

A ferramenta também sugeriu trocar o `eventoId` para UUID em vez de ID sequencial, e
adicionar índice único no banco.

**O que foi aceito:** a verificação de evento já processado antes de aplicar a lógica de
negócio, o registro do evento como processado dentro da mesma transação do efeito
(atomicidade via `@Transactional`), e o índice único na tabela `eventos_processados`.

**O que foi recusado, e por quê:**

- **Trocar `eventoId` para UUID**: o `eventoId` é definido no serviço produtor e viaja no
  header `ce_id` do envelope CloudEvents. Mudar sua geração no consumidor quebraria o
  contrato entre os dois serviços, e não há necessidade real de trocar um identificador já
  definido só para adotar UUID — o formato do identificador é decisão do produtor, não do
  consumidor.
- **Ignorar duplicatas silenciosamente**: retornar sem nenhum registro faz o time perder
  observabilidade sobre quantas vezes um evento chegou duplicado. Optou-se por manter um
  log em nível INFO quando um evento é ignorado por já ter sido processado, em vez de
  descartar a informação.

---

### Interação 3 — Circuit breaker para o banco de dados

**O que foi pedido:** proteger o serviço de ocupação contra falhas do Postgres, evitando
que uma queda do banco represasse o consumo do Kafka.

**O que a ferramenta sugeriu:**

```java
@Service
public class OcupacaoService {

    private final OcupacaoRepository repository;

    @CircuitBreaker(name = "ocupacaoService", fallbackMethod = "fallbackProcessar")
    @Retry(name = "ocupacaoService")
    @Transactional
    public void processar(AgendamentoConfirmadoEvent evento) {
        if (repository.eventoJaProcessado(evento.getEventoId())) {
            return;
        }
        repository.registrarEventoProcessado(evento.getEventoId());
        repository.registrarOcupacao(
            evento.getAgendamentoId(),
            evento.getProfissionalId(),
            evento.getInicioEm()
        );
    }

    public void fallbackProcessar(AgendamentoConfirmadoEvent evento, Exception e) {
        System.out.println("Circuit aberto, evento descartado: " + evento.getEventoId());
    }
}
```

A ferramenta também sugeriu configurar Resilience4j no `application.yml` com
`failureRateThreshold = 50%`.

**O que foi aceito:** a ideia geral do padrão Circuit Breaker como proteção contra falha em
cascata do banco, e a existência de um método de fallback para degradação controlada em vez
de deixar a exceção subir sem tratamento.

**O que foi recusado, e por quê:**

- **Descartar o evento no fallback**: descartar uma mensagem do Kafka é uma perda
  irreversível de dado de negócio (um agendamento que nunca entraria na projeção de
  ocupação). A decisão da equipe foi não adotar circuit breaker nesta etapa da atividade —
  o padrão correto exigiria publicar o evento numa Dead Letter Topic no fallback, o que é
  um escopo maior do que o pedido pela Parte B desta aula.
- **`@Retry` e `@Transactional` no mesmo método com `@CircuitBreaker`**: o circuit breaker
  reexecuta o método inteiro em caso de falha; como a transação já registra o evento como
  processado antes de retornar, uma reexecução colidiria com a constraint de chave primária
  de `eventos_processados`. Combinar os três na mesma assinatura de método esconderia esse
  conflito em vez de resolvê-lo.
- **`failureRateThreshold` de 50%**: para uma dependência crítica como o banco que registra
  a deduplicação, 50% de falha antes de abrir o circuito é alto demais — significaria
  perder metade dos eventos antes de qualquer proteção entrar em ação. Um valor adequado
  exigiria também configurar `minimumNumberOfCalls`, para não abrir o circuito com poucas
  chamadas em ambiente de baixo volume como o da atividade.

---

### Interação 4 — Primeiro agregador por janela

**O que foi pedido:** criar um novo consumidor no mesmo tópico, com group.id próprio, para
agregar confirmações em janelas de 15 minutos.

**O que a ferramenta sugeriu:** usar `processing time` e publicar apenas um log resumido
da agregação, sem persistir o resultado em uma projeção consultável.

**O que foi aceito:** a janela alinhada ao relógio do sistema e o uso de um segundo
consumidor com group.id separado, consumindo o mesmo tópico da Etapa 1.

**O que foi recusado, e por quê:**

- **`processing time` como relógio principal**: a pergunta de negócio escolhida depende de
  quando a confirmação aconteceu no salão, e não de quando o broker entregou a mensagem.
  Usar processamento introduziria distorção se a fila atrasasse, justamente o cenário que a
  agregação precisa medir.
- **Somente log como saída**: um log ajuda na inspeção manual, mas não sustenta consulta
  histórica nem validação simples da projeção. A equipe optou por persistir a agregação em
  PostgreSQL, mantendo o resultado observável e recuperável depois.
