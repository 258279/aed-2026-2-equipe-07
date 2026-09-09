# Contrato do evento `salao.agendamento.confirmado.v1`

Este é o tipo do evento publicado pelo serviço de agendamentos e consumido pelos serviços da equipe.
O evento representa um fato já ocorrido no domínio: o agendamento foi confirmado e passou a
ocupar o horário do profissional. Ele não representa uma solicitação, uma tentativa de
confirmação ou um pagamento aprovado.

O produtor publica a confirmação depois de validar a disponibilidade do horário e concluir o
fluxo de confirmação definido para a etapa atual. Os consumidores podem projetar a ocupação e
agregar confirmações sem precisar chamar o produtor de forma síncrona.

## Campos do payload

| Campo            | Tipo   | Obrigatório | Significado                                                                                                                                          |
| ---------------- | ------ | ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `eventoId`       | string | sim         | Identidade desta ocorrência de confirmação. Permite rastrear o fato e distinguir uma nova confirmação de uma reentrega da mesma mensagem. Consumidores idempotentes usam este valor como chave de deduplicação. |
| `agendamentoId`  | string | sim         | Identidade do compromisso que foi confirmado. Também é a chave de partição do tópico, preservando a ordem dos eventos do mesmo agendamento; não deve ser usada sozinha para deduplicar confirmações. |
| `profissionalId` | string | sim         | Identidade do profissional que atenderá o cliente no horário confirmado. É usada para construir a projeção de ocupação e não representa necessariamente a identidade de quem realizou a confirmação. |
| `servicoId`      | string | sim         | Identidade do serviço de salão reservado. Permite análises por tipo de serviço; alterar seu significado sem alterar o tipo poderia produzir relatórios incompatíveis com o histórico. |
| `inicioEm`       | string | sim         | Data e hora em que o atendimento está previsto para começar, em ISO-8601 com offset. É o horário reservado, não o instante em que a confirmação foi registrada. |
| `prioridade`     | string | sim         | Classificação de negócio atribuída ao agendamento, como `PADRAO` ou `URGENTE`. É usada para triagem e para a agregação operacional da demanda. |
| `ocorridoEm`     | string | sim         | Data e hora em que a confirmação aconteceu no negócio, em ISO-8601 com offset. É o relógio do fato e a base das agregações por janela, independentemente do atraso de entrega pelo broker. |

## Datas

Todos os campos temporais são representados como ISO-8601 com offset, nunca como epoch. Exemplos válidos: `2026-08-22T14:00:00-03:00` e `2026-08-16T12:30:00-03:00`.

## Partição e ordem

A chave de partição é `agendamentoId`.

Essa escolha garante ordem apenas para eventos do mesmo agendamento. Eventos de agendamentos diferentes podem ser consumidos em paralelo, o que é aceitável porque a ordem de um agendamento não precisa depender da ordem de outro.

## Compatibilidade

A regra escolhida é **BACKWARD**.

Justificativa: o produtor já existe e os consumidores são implantados independentemente. Manter compatibilidade backward permite adicionar campos no futuro sem quebrar consumidores antigos, desde que a evolução seja aditiva. Isso é coerente com a etapa atual, em que o consumidor da Etapa 1 continua lendo o mesmo evento enquanto um novo consumidor passa a usar o mesmo tópico. Remoções e renomeações de campo exigiriam uma nova versão do evento.

Na implantação, os consumidores devem ser atualizados antes do produtor quando uma mudança
aditiva for necessária. Assim, os consumidores passam a aceitar o contrato ampliado antes que
o produtor comece a enviar os novos campos. Consumidores devem ignorar campos desconhecidos e
continuar usando apenas os campos necessários para sua responsabilidade.

Compatibilidade de esquema não preserva compatibilidade semântica. Adicionar um campo é
compatível quando seu significado é novo e independente. Alterar o significado de um campo
existente, mesmo mantendo o tipo `string`, pode fazer um consumidor antigo produzir uma
projeção incorreta. Por exemplo, reutilizar `inicioEm` para representar o instante da
confirmação confundiria o horário reservado com `ocorridoEm`; nesse caso, deve-se manter o
significado atual e adicionar outro campo ou publicar uma nova versão do evento.

As seguintes mudanças são consideradas incompatíveis com `v1`:

- remover ou renomear um campo obrigatório;
- mudar o significado de um campo existente;
- mudar o tipo ou o formato de uma data;
- alterar a semântica de `eventoId` ou `agendamentoId`;
- mudar o significado de `prioridade` de forma que os valores atuais deixem de ser válidos.

Essas mudanças devem resultar em um novo tipo versionado, como
`salao.agendamento.confirmado.v2`, com documentação própria e período de coexistência para os
consumidores.

## Exemplo de payload

```json
{
  "eventoId": "EVT-20260816-0001",
  "agendamentoId": "AG-20260822-0142",
  "profissionalId": "PROF-102",
  "servicoId": "SERV-004",
  "inicioEm": "2026-08-22T14:00:00-03:00",
  "prioridade": "URGENTE",
  "ocorridoEm": "2026-08-16T12:30:00-03:00"
}
```
