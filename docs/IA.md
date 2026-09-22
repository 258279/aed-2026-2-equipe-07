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
`OcupacaoService` por construtor e a captura do header `ce_id` para rastreabilidade do
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

A ferramenta também sugeriu configurar o Resilience4j no `application.yml` com
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
- **Somente log como saída**: um log ajuda na inspeção manual, mas não sustenta uma consulta
  histórica nem validação simples da projeção. A equipe optou por persistir a agregação em
  PostgreSQL, mantendo o resultado observável e recuperável depois.

---

### Interação 5 — Retry, backoff, DLQ e reprocessamento

**O que foi pedido:** implementar retry com backoff exponencial, envio para uma Dead Letter
Topic e um mecanismo de reprocessamento manual, para os dois consumidores Kafka do
`servico-ocupacao` (o da Etapa 1 e o agregador por janela).

**O que a ferramenta sugeriu:**

```java
ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(NUMERO_DE_TENTATIVAS);
backOff.setInitialInterval(INTERVALO_INICIAL_MS);
backOff.setMultiplier(MULTIPLICADOR);

DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
        kafkaTemplate,
        (record, ex) -> new TopicPartition(topicoOriginal + ".dlq." + groupId, -1)
);

DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
errorHandler.addNotRetryableExceptions(JacksonException.class);
```

Antes de qualquer código, a ferramenta levantou quatro decisões em aberto e apresentou opções
com trade-offs para cada uma, em vez de decidir sozinha: como classificar uma mensagem
malformada (retry igual às outras falhas ou direto para a DLQ), estratégia de backoff (fixo ou
exponencial), como reprocessar a DLQ (endpoint manual, job agendado automático ou apenas
documentar o comando de console do Kafka) e como nomear o(s) tópico(s) de DLQ (um único
compartilhado ou um por consumidor). A implementação final usa um `DefaultErrorHandler` por
`containerFactory` (um por `group.id`), 3 tentativas com backoff 1s/2s/4s e um
`DlqReprocessamentoService` com um `KafkaConsumer` avulso em `group.id` dedicado
(`<group-id>-dlq-reprocessador`), exposto por um
`DlqController` (`POST /admin/dlq/{consumidor}/reprocessar`).

**O que foi aceito:** a estratégia geral (`DefaultErrorHandler` +
`ExponentialBackOffWithMaxRetries` + `DeadLetterPublishingRecoverer`, um por consumidor),
backoff exponencial em vez de fixo (mais gentil com uma dependência se recuperando), 3
tentativas antes de desistir e um endpoint administrativo único cobrindo os dois consumidores
por meio de um parâmetro de rota.

**O que foi recusado, e por quê:**

- **Tratar uma mensagem malformada igual a qualquer outra falha (retry com backoff):** um erro de
  parsing de JSON é determinístico — a mesma mensagem malformada vai falhar da mesma forma na
  1ª, na 2ª e na 4ª tentativa, porque o problema é o conteúdo da mensagem, não uma dependência
  externa instável. Gastar os 3 retries (7s de backoff) nesse caso só atrasa a chegada à DLQ
  sem nenhuma chance real de sucesso. A equipe optou por classificar erro de parsing como não
  retentável (`addNotRetryableExceptions`), reservando o orçamento de retry só para falhas que
  podem mesmo se resolver sozinhas (por exemplo, banco indisponível).
- **Reprocessamento automático da DLQ via job agendado:** a ferramenta apresentou essa opção
  como alternativa ao endpoint manual. Foi recusada porque reprocessar automaticamente uma
  mensagem que já falhou pode recolocá-la em loop de falha indefinidamente se a causa raiz
  (o bug que a fez cair na DLQ) ainda não tiver sido corrigida — sem um humano no meio, o
  sistema ficaria tentando reprocessar o mesmo erro sem parar. A decisão da equipe foi manter o
  reprocessamento sob controle humano explícito (`POST /admin/dlq/{consumidor}/reprocessar`),
  chamado somente depois de confirmar que a causa raiz foi corrigida.
- **Um único tópico de DLQ compartilhado entre os dois consumidores:** mais simples de operar
  (um lugar só pra olhar), mas mistura as falhas de dois processamentos independentes na mesma
  fila. Como os dois consumidores (`servico-ocupacao` e `servico-ocupacao-agregacao-janelas`)
  têm donos e causas de falha diferentes, um DLQ compartilhado dificultaria saber qual consumidor
  quebrou e reprocessar cada um separadamente. A equipe optou por um tópico de DLQ por
  consumidor (`<tópico-original>.dlq.<group-id>`), mesmo custando um pouco mais de operação.
- **`DlqReprocessamentoService` sem `enable.auto.commit=false` explícito:** a primeira versão
  gerada pela ferramenta criava o `KafkaConsumer` avulso do reprocessador sem desligar o
  auto-commit do Kafka. A equipe revisou o código e percebeu que, como o padrão do Kafka é
  `enable.auto.commit=true` a cada 5s, o offset da DLQ podia ser commitado automaticamente antes
  (ou independentemente) da republicação no tópico original ter sido de fato confirmada — se o processo caísse nesse intervalo, a mensagem seria dada como consumida da DLQ sem nunca ter sido republicada, uma perda silenciosa de dado. Foi recusada e corrigida desligando o auto-commit, mantendo como único commit o `commitSync()` explícito, chamado só depois que todas as republicações da leva já haviam sido confirmadas — a equipe preferiu essa garantia mesmo sem o pedido original ter especificado esse detalhe.

### Interação 6 — Chave de partição e evidência da Aula 04

**O que foi pedido:** adaptar a entrega ao problema de agregação por cliente em ambiente com múltiplas instâncias, documentar a janela usada e registrar uma decisão sobre repartition.

**O que a IA sugeriu:** escolher `clienteId` como chave do fluxo de agregação,
usando um repartition topic, e validar a concorrência com duas instâncias do
agregador. A análise também recomendou conferir o contrato antes de implementar um listener de repartição que preserve corpo e headers CloudEvents.

**O que foi aceito:** o ADR-003 registra `clienteId` como decisão arquitetural
para consultas por cliente e explicita os custos de um serviço, um tópico, uma
passagem extra, idempotência e partição quente. Também foi aceito o teste com
duas instâncias independentes do serviço atual, que confirma a contagem correta na mesma janela e a deduplicação por `eventoId`. A documentação da Aula 04
registra, de forma verificável, que a implementação atual é tumbling window de
15 minutos por prioridade e usa `ocorridoEm` como event time.

**O que foi recusado, e por quê:**

- **Implementar imediatamente o repartitionador com `clienteId`:** recusado
  porque o contrato atual não possui esse campo. Descobrir a chave por
  desserialização e recriar o evento mudaria o desenho exigido de preservar
  corpo e headers intactos; inventar uma chave fora do contrato produziria uma
  implementação que parece completa, mas não atende ao domínio real. A equipe
  preferiu documentar a decisão, exigir a evolução backward-compatible do
  contrato e deixar a implementação para quando houver uma fonte legítima de
  `clienteId`.
- **Declarar que já existe session window por cliente:** recusado porque o
  código existente implementa tumbling window por prioridade. A documentação
  mantém essa distinção para que a evidência de teste não seja maior que o
  comportamento realmente executado.

---

## Aula 05

### Interação 7 — Contraditório: razões contra Event Sourcing no agregado Agendamento

**O que foi pedido:** simular um contraditório, pedindo deliberadamente à IA para listar
razões CONTRA a decisão de usar Event Sourcing no agregado Agendamento, antes de fechar a
implementação.

**O que a IA apontou contra a decisão:**

- O volume real do domínio é baixíssimo (no máximo 3 eventos por agendamento: Confirmado,
  e depois Cancelado+SinalEstornado ou NaoCompareceu+MultaCobrada). Event Sourcing costuma
  compensar quando o histórico é longo ou consultado com frequência; para 3 eventos, um log
  de auditoria simples ao lado de uma tabela de estado tradicional resolveria a
  rastreabilidade sem o custo de reconstruir estado por fold.
- A curva de aprendizado da equipe é um custo pago imediatamente, enquanto o benefício
  (auditoria robusta numa disputa sobre prazo) só se manifesta se esse cenário realmente
  acontecer.
- A complexidade de LGPD com crypto-shredding introduz gestão de chave por cliente que não
  existiria com um UPDATE simples mais um campo `anonimizado_em`.

**O que foi aceito:** o reconhecimento de que o volume atual não justificaria Event Sourcing
sozinho — por isso a decisão foi mantida restrita a um único agregado, e não expandida para
o resto do sistema, como já registra a ADR-005.

**O que foi recusado, e por quê:** a alternativa de log de auditoria separado foi recusada
mesmo com o argumento de menor esforço, porque ela cria duas fontes de verdade (o log e o
estado gravado) que podem divergir sem nada forçar consistência entre elas — esse é
justamente o problema que motivou a ADR-005. O baixo volume atual não elimina o risco de
divergência, só o torna menos visível. A equipe decidiu que um caminho de exceção com
implicação financeira (estorno de sinal, cobrança de multa) exige uma única fonte de
verdade, mesmo com o custo de curva de aprendizado maior.

---

## Aula 06

### Interação 8 — Pergunta-armadilha: como corrigir um evento de compensação já publicado?

**O que foi pedido:** pedir deliberadamente à IA uma sugestão para um cenário clássico de
erro nesta aula: "publicamos o evento `AgendamentoCanceladoPorConflitoEvent` com o
`agendamentoId` errado, como eu corrijo isso?"

**O que a IA sugeriu:** a resposta inicial sugeriu fazer um UPDATE direto no registro já
gravado (ou, alternativamente, apagar e republicar o evento com o mesmo `ce_id`),
argumentando que seria mais simples do que criar um novo tipo de evento só para uma
correção pontual.

**O que foi aceito:** nada da sugestão inicial foi aceito da forma como veio.

**O que foi recusado, e por quê:** UPDATE/DELETE num evento já publicado quebra a regra
central do desenho de resiliência do projeto: consumidores já processaram (e possivelmente
já compensaram) esse evento, e alterar o passado silenciosamente pode deixar reprocessamentos
futuros inconsistentes sem que nenhum consumidor perceba — o log deixaria de ser uma fonte
confiável do que realmente aconteceu, a mesma razão pela qual a Saga já usa eventos de
compensação em vez de UPDATE no agregado (ADR-006). Reaproveitar o `ce_id` original também
quebraria a deduplicação por idempotência de quem já consumiu o evento errado. A equipe
recusou as duas sugestões e decidiu que a correção correta é publicar um novo evento (ex.: um
evento de correção referenciando o `ce_id` original como causa), preservando o histórico
completo — inclusive do erro e da correção — como registro imutável.
