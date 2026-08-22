# Contrato do evento `salao.agendamento.confirmado.v1`

Este é o tipo do evento publicado pelo serviço de agendamentos e consumido pelos serviços da equipe.

## Campos do payload

| Campo            | Tipo   | Obrigatório | Significado                                                                                                                                          |
| ---------------- | ------ | ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `eventoId`       | string | sim         | Identificador único do evento, usado para rastreabilidade e, em consumidores idempotentes, para evitar processamento duplicado.                      |
| `agendamentoId`  | string | sim         | Identificador do agendamento confirmado; também é a chave de partição do tópico para manter a ordem dos eventos do mesmo agendamento.                |
| `profissionalId` | string | sim         | Identificador do profissional que ficou vinculado ao horário confirmado.                                                                             |
| `servicoId`      | string | sim         | Identificador do serviço de salão associado ao agendamento; se este campo mudar, relatórios por serviço podem deixar de representar o mesmo negócio. |
| `inicioEm`       | string | sim         | Data e hora de início do atendimento confirmado, em ISO-8601 com offset.                                                                             |
| `prioridade`     | string | sim         | Classe de prioridade do agendamento, usada pelo negócio para triagem e análise de demanda.                                                           |
| `ocorridoEm`     | string | sim         | Instante em que a confirmação aconteceu no negócio, em ISO-8601 com offset; é a base temporal correta para agregações por janela.                    |

## Datas

Todos os campos temporais são representados como ISO-8601 com offset, nunca como epoch. Exemplos válidos: `2026-08-22T14:00:00-03:00` e `2026-08-16T12:30:00-03:00`.

## Partição e ordem

A chave de partição é `agendamentoId`.

Essa escolha garante ordem apenas para eventos do mesmo agendamento. Eventos de agendamentos diferentes podem ser consumidos em paralelo, o que é aceitável porque a ordem de um agendamento não precisa depender da ordem de outro.

## Compatibilidade

A regra escolhida é **BACKWARD**.

Justificativa: o produtor já existe e os consumidores são implantados independentemente. Manter compatibilidade backward permite adicionar campos no futuro sem quebrar consumidores antigos, desde que a evolução seja aditiva. Isso é coerente com a etapa atual, em que o consumidor da Etapa 1 continua lendo o mesmo evento enquanto um novo consumidor passa a usar o mesmo tópico. Remoções e renomeações de campo exigiriam uma nova versão do evento.

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
