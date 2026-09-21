# ADR-006 - Resiliencia: retry, DLQ e Saga de compensacao

## Contexto

O sistema possui dois fluxos assincronos principais:

1. consumo de `salao.agendamento-confirmado` no `servico-ocupacao` (projecao de ocupacao);
2. consumo de `salao.agendamento-cancelado-por-conflito` no `servico-agendamentos` (reacao a compensacao).

Os fluxos precisavam distinguir falhas transitorias de permanentes para evitar bloqueio de
particao e crescimento de lag sem progresso.

Tambem era necessario explicitar um caminho de excecao no dominio: quando chega uma
confirmacao para o mesmo `profissionalId` e `inicioEm` ja ocupado por outro agendamento,
a confirmacao inicial precisa ser compensada com um novo evento de dominio (nunca
`UPDATE`/`DELETE` no log).

## Decisao

1. Politica de retry e backoff:
- foi adotado `DefaultErrorHandler` com `ExponentialBackOffWithMaxRetries(3)`;
- espera: 1s, 2s, 4s (backoff exponencial);
- justificativa: protege dependencias em recuperacao sem criar tempestade de retries.

2. Classificacao de falha no dominio:
- nao retentaveis (permanentes): `JacksonException` e `IllegalArgumentException`;
- retentaveis (transitorias): demais excecoes tecnicas (ex.: indisponibilidade temporaria de infra).

3. DLQ por consumidor (topico derivado + group id):
- `salao.agendamento-confirmado.dlq.servico-ocupacao`;
- `salao.agendamento-confirmado.dlq.servico-ocupacao-agregacao-janelas`;
- `salao.agendamento-cancelado-por-conflito.dlq.servico-agendamentos-compensacao`.

Monitoracao operacional:
- DLQs de `salao.agendamento-confirmado.*`: equipe do servico de ocupacao;
- DLQ da compensacao: equipe do servico de agendamentos.

4. Reprocessamento manual e idempotente:
- mantido endpoint administrativo de reprocessamento no `servico-ocupacao`;
- ao republicar, payload e headers sao preservados, incluindo CloudEvents;
- seguranca funcional vem da idempotencia por `eventoId` no consumidor.

5. Saga com compensacao (coreografia):
- estilo escolhido: **coreografia**;
- detector da falha: `servico-ocupacao`;
- evento de compensacao publicado: `salao.agendamento.cancelado-por-conflito.v1`;
- consumidor da compensacao: `servico-agendamentos`, que projeta status para
  `CANCELADO_POR_CONFLITO`.

6. Falha da propria compensacao:
- o consumidor de compensacao tambem usa retry + backoff + DLQ propria;
- consumo idempotente por `eventoId` evita efeito duplicado em reentregas;
- dono operacional da DLQ de compensacao: equipe de agendamentos;
- prazo de tratamento aceito: ate o proximo dia util.

## Alternativas consideradas

1. Retry para toda falha, inclusive permanentes
- rejeitada porque mensagem malformada/regra violada nao melhora com nova tentativa;
- efeito colateral: bloqueio da particao e aumento de lag sem progresso.

2. Reprocessamento automatico em loop da DLQ
- rejeitada por risco de ciclo infinito antes de corrigir causa raiz;
- preferimos acao manual explicita para manter controle operacional.

3. Orquestracao central da Saga
- rejeitada para o escopo atual por custo adicional de componente coordenador;
- para este fluxo curto (confirmacao -> conflito -> compensacao), coreografia foi suficiente.

4. Compensacao por chamada sincrona entre servicos
- rejeitada por acoplamento temporal e por aproximar transacao distribuida;
- evento assincrono manteve independencia de deploy e de disponibilidade.

## Consequencias aceitas

1. Piora de latencia em falhas transitorias
- com retries de 1s, 2s e 4s, um evento pode levar ate ~7s extras antes de ir para DLQ.

2. Aumento de custo operacional
- mais topicos para monitorar (DLQs por consumidor e DLQ da compensacao);
- maior carga de observabilidade e rotina operacional.

3. Complexidade adicional de codigo
- mais fluxos para testar: caminho feliz, caminho de DLQ, reprocessamento e compensacao;
- mais classes e configuracoes por servico.

4. Coreografia reduz visibilidade linear do processo
- sem um orquestrador central, rastrear ponta a ponta depende mais de correlacao via
  `ce_id`/`eventoId` e observabilidade por topico.

5. Risco de backlog em DLQ se houver disciplina operacional fraca
- o design depende de rotina de triagem/reprocessamento manual no prazo aceito.

## Status

Aceito.
