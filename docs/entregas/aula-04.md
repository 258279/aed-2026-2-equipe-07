# Aula 04 - Programação reativa e backpressure

## Agregação implementada

No domínio da equipe, o agregador existente conta confirmações de agendamento
por `prioridade` e por intervalo de tempo. A saída é persistida em
`agregacao_confirmacoes_por_janela`, com deduplicação por `eventoId` para
reentregas do Kafka.

## Janela escolhida

A implementação usa uma janela **tumbling** de 15 minutos, alinhada ao início
do quarto de hora. O relógio é o `event time` de `ocorridoEm`, e não o instante
de processamento: uma confirmação atrasada atualiza a janela histórica correta.

Essa janela foi escolhida porque a pergunta operacional é quantas confirmações
chegaram em cada período fixo de 15 minutos, separadas por prioridade. Uma
session window seria adequada para agrupar atividade de um cliente até um
período de inatividade, mas o contrato atual não possui `clienteId`, e o código
existente não implementa essa semântica. Hopping ou sliding acrescentariam
sobreposição desnecessária para o relatório atual.

## Evidência com múltiplas instâncias

O teste `deveManterAgregacaoConsistenteComDuasInstanciasCompartilhandoAProjecao`
cria duas instâncias independentes de `AgendamentoConfirmadoJanelaService`,
simula cada uma processando um evento distinto e verifica que a mesma janela
termina com contagem 2. Os testes de reentrega também verificam que o mesmo
`eventoId` não é contado duas vezes.

Essa evidência valida a concorrência sobre a projeção compartilhada e a
idempotência do agregador. Ela não afirma que existe hoje uma session window por
cliente em Kafka: essa parte depende da evolução do contrato com `clienteId` e
do repartition topic decidido no ADR-003.
