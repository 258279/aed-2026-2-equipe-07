# ADR-002 — Domínio do projeto

## Status

Aceita · 2026-08-16 · Equipe 07

## Contexto

A equipe (Diego Marques, Julio Fernandes, Gabriel Yuji, Lucas Lisboa, William Tavares e João Vítor Vieira)
discutiu coletivamente e escolheu o domínio de agendamento de salão de beleza por ser um
processo de negócio real e reconhecível, com estrutura rica o suficiente para sustentar os
conceitos de arquitetura reativa e orientada a eventos trabalhados na disciplina — em
especial por reunir naturalmente decisão, sistema externo, compensação e reprocessamento
no mesmo fluxo, o que o time avaliou como mais produtivo do que um domínio inventado sem
lastro na prática.

O problema que a equipe escolheu resolver é a gestão de agendamentos em um salão de beleza
com horário marcado, onde overbooking, faltas e cancelamentos de última hora geram prejuízo
direto (horário ocioso, profissional parado, receita perdida).

Escolhemos este domínio agora porque ele nasce de um processo que já existe informalmente
(agenda de papel, WhatsApp, planilha) em muitos negócios pequenos, e formalizá-lo como
fluxo orientado a eventos expõe naturalmente os quatro elementos que a disciplina exige.

## Decisão

O cliente solicita um horário; o sistema verifica disponibilidade e prioridade (ex.: plano
do cliente, urgência do atendimento) e confirma ou recusa a reserva; a confirmação exige o
pagamento de um sinal via gateway externo. Se o cliente cancela dentro do prazo, o sinal é
estornado; se não comparece (no-show), uma multa é cobrada. Periodicamente o sistema gera
projeções de ocupação e relatórios de falta para o negócio.

Como o domínio atende cada um dos quatro critérios:

- **ponto de decisão com regra de negócio**: confirmar, recusar por conflito de horário, ou
  priorizar a reserva conforme urgência/plano do cliente
- **sistema externo**: gateway de pagamento (cobrança do sinal) e serviço de notificação
  (SMS/e-mail de confirmação e lembrete)
- **caminho de exceção com compensação**: cancelamento dentro do prazo → estorno do sinal;
  no-show → cobrança de multa
- **algo que valha reprocessar**: projeção de ocupação futura e relatório consolidado de
  faltas, construídos a partir do histórico de eventos

## Alternativas consideradas

- **Cadastro simples de clientes/serviços (CRUD)**: descartado por não ter ponto de decisão
  real nem caminho de compensação — é transporte de dados, não um processo de negócio.
- **Pedido → estoque (Mercado Rápido)**: descartado por ser o recorte que a demonstração
  (`demo-kafka-idempotencia`) já cobre integralmente.
- **Fila de espera para atendimento sem hora marcada (tipo "pegar senha")**: descartado por
  não ter um sistema externo relevante nem um caminho de exceção com compensação claro — a
  única "regra" é ordem de chegada, o que empobrece o modelo de eventos e não sustenta Saga
  nem Event Sourcing nas aulas seguintes.

## Consequências aceitas

- Vamos ter que modelar o conceito de "prioridade" de forma explícita no evento de reserva
  (ex.: plano do cliente, urgência), o que aumenta a complexidade do agregado desde já.
- O fluxo de sinal/estorno cria dependência de um gateway de pagamento externo — na aula 05,
  isso provavelmente vai exigir uma Saga para coordenar reserva + cobrança + possível estorno
  de forma consistente.
- Ficam fora do escopo desta etapa: reagendamento (troca de horário sem cancelar), fila de
  espera para horários lotados, e cobrança proporcional em cancelamentos parciais — esses
  pontos tendem a aparecer como exceções adicionais na aula 05 e podem exigir revisão do
  modelo de eventos.
- O relatório de faltas e a projeção de ocupação dependem de um histórico consistente de
  eventos; se o consumidor não for realmente idempotente desde a Parte B, esses relatórios
  ficam distorcidos — isso reforça a idempotência como requisito crítico, não só de nota.
