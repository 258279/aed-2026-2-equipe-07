# ADR-003 - Chave de partição para agregações por cliente

## Contexto

O evento `salao.agendamento-confirmado.v1` é publicado atualmente com
`agendamentoId` como chave de partição. Essa chave preserva a ordem dos eventos de
um agendamento, mas não garante que eventos de um mesmo cliente, associados a
agendamentos diferentes, caiam na mesma partição. Com várias instâncias do
consumidor, uma session window por cliente pode ser dividida entre instâncias e
produzir uma sessão incompleta.

O contrato atual ainda não possui `clienteId`; portanto, esta decisão define o
alvo arquitetural e requer a evolução backward-compatible do contrato antes da
implantação do repartitionador.

## Decisao

Escolhemos `clienteId` como chave do fluxo destinado a agregações por cliente,
por meio de um tópico de repartition. O repartitionador deve ler o evento
original, preservar corpo e headers CloudEvents, e republicar o mesmo registro
com apenas a chave alterada para `clienteId`.

Essa chave responde diretamente à pergunta: **quantas confirmações e qual
atividade de agenda pertencem a cada cliente durante uma sessão?** Todas as
mensagens de um cliente passam a ser encaminhadas para a mesma partição e podem
ser processadas pela mesma instância do agregador.

## Alternativas consideradas

- **Manter `agendamentoId`**: preserva a ordem por agendamento, mas não resolve a
  agregação por cliente em várias partições.
- **Executar uma única instância do agregador**: evita a divisão entre
  instâncias, mas remove escalabilidade e cria um ponto único de saturação ou
  indisponibilidade.
- **Usar chave composta, como `clienteId:agendamentoId`**: preserva uma relação
  mais específica, mas espalha novamente os agendamentos do mesmo cliente e não
  garante uma session window por cliente.

## Consequencias aceitas

- Será necessário um serviço/listener adicional e um tópico de repartition.
- O fluxo terá uma passagem extra pelo broker, aumentando a latência e o custo
  operacional.
- A entrega pelo menos uma vez exige idempotência no repartitionador e no
  agregador para tolerar republicações e reentregas.
- Clientes muito ativos podem concentrar tráfego em uma partição quente; a
  chave por cliente troca paralelismo por afinidade da sessão.
- O contrato precisa expor `clienteId` de forma compatível antes da ativação.
- A chave por cliente responde consultas de sessão e total por cliente sem nova
  repartição, mas deixa de responder com afinidade a perguntas por agendamento
  individual; essas consultas podem exigir uma chave ou projeção adicional.
- O repartitionador ainda não foi implementado nesta entrega porque o payload
  atual não fornece `clienteId`; inferir a chave desserializando ou recriando o
  payload violaria a preservação exigida de corpo e headers.

## Status

Aceito.
