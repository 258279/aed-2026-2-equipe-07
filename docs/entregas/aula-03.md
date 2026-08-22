# Aula 03 — Etapa 2

## 1. Pergunta de negócio

Quantas confirmações de agendamento o salão recebe por prioridade em janelas de 15 minutos? Essa visão ajuda a perceber picos operacionais e a diferença entre confirmações normais e urgentes ao longo do dia.

## 2. Relógio escolhido

Escolhemos **event time / tempo de ocorrência**. O campo `ocorridoEm` representa quando a confirmação aconteceu no negócio, e não quando o Kafka entregou a mensagem. Como a pergunta é sobre o comportamento real da agenda do salão, a janela precisa seguir o tempo de ocorrência, não o atraso de transporte.

## 3. Evento atrasado

Se um evento chegar atrasado, ele é classificado pela janela derivada de `ocorridoEm`. Ou seja, ele atualiza a janela histórica correta, mesmo que a mensagem tenha sido consumida depois do fechamento daquela janela no relógio do sistema.

## 4. Reprocessamento

Sim, o resultado é o mesmo se o fluxo for reprocessado do começo em uma projeção vazia. A janela, a prioridade e a contagem dependem somente do conteúdo do evento, então a agregação é determinística. A consequência prática é que a projeção precisa ser reconstruída do zero no replay; isso é aceitável porque o resultado desejado é justamente uma visão derivada do histórico.
