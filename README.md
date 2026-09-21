# Equipe 07

### Líder: João Vítor Vieira Martins

### Integrantes:

(254801) - Diego Cardoso Marques

(254428) - Gabriel Yuji Yasuda Cardoso

(258850) - João Vítor Vieira Martins

(255323) - Júlio César Fernandes

(258279) - Lucas Gabriel Lisboa Alves

(255180) - William Tavares de Moura

## Domínio

Agendamento de horários em salão de beleza, com confirmação, cobrança de sinal e controle de ocupação dos profissionais via eventos.

## Como subir (ambiente com Docker Compose)

Requisitos:

- Docker
- Docker Compose

No repositório há um `docker-compose.yml` que fornece Zookeeper, Kafka, Postgres e constrói os dois serviços.

Para subir tudo em uma máquina limpa:

```bash
git clone <url-do-repositorio>
cd aed-2026-2-equipe-07
docker compose up --build
```

Verificações rápidas:

- Kafka: `localhost:9092` está exposto
- Servico Agendamentos: `http://localhost:8080`
- Servico Ocupacao: `http://localhost:8081`

O Postgres é inicializado com as tabelas necessárias via `docker/postgres-init/init.sql`.

## Consumidor adicional da Aula 03

O `servico-ocupacao` passou a executar dois consumidores Kafka no mesmo tópico `salao.agendamento-confirmado`:

- o consumidor da Etapa 1, responsável pela projeção de ocupação por agendamento;
- o novo agregador por janela de 15 minutos, com `group.id` próprio (`servico-ocupacao-agregacao-janelas`).

Para subir tudo com o novo agregador, continue usando o mesmo comando da raiz:

```bash
docker-compose up --build
```

Como observar o resultado da agregação:

```sql
SELECT janela_inicio, janela_fim, prioridade, quantidade_confirmacoes
FROM agregacao_confirmacoes_por_janela
ORDER BY janela_inicio, prioridade;
```

As datas do evento continuam em ISO-8601 com offset, e a janela do agregador é derivada de
`ocorridoEm`.

## Resiliência: retry, DLQ e reprocessamento

O `servico-ocupacao` trata falha de processamento nos dois consumidores (o da Etapa 1 e o
agregador por janela):

- até 3 tentativas com backoff exponencial (1s, 2s, 4s) para falhas transientes (ex: banco
  fora do ar);
- erro de parsing do JSON (mensagem malformada) vai direto para a fila morta, sem gastar
  tentativas de retry;
- depois de esgotar as tentativas, a mensagem vai para um tópico de DLQ próprio de cada
  consumidor:
  - `salao.agendamento-confirmado.dlq.servico-ocupacao`
  - `salao.agendamento-confirmado.dlq.servico-ocupacao-agregacao-janelas`

Para reprocessar manualmente o que está numa DLQ (depois de corrigir a causa raiz):

```bash
curl -X POST http://localhost:8081/admin/dlq/ocupacao/reprocessar
curl -X POST http://localhost:8081/admin/dlq/janela/reprocessar
```

Cada chamada devolve `{"reprocessados": N}`. Se a DLQ tiver mais mensagens do que uma chamada
consegue drenar de uma vez, chame de novo até a resposta vir com `0`.

Esse endpoint não tem autenticação — é uma superfície de administração para uso local/operacional,
não algo pra expor assim num ambiente real.

## Saga de compensação por conflito de horário

Quando o `servico-ocupacao` recebe uma confirmação para um horário já ocupado pelo mesmo
profissional, ele publica um novo evento de compensação (fato no particípio):

- tópico: `salao.agendamento-cancelado-por-conflito`
- tipo CloudEvents: `salao.agendamento.cancelado-por-conflito.v1`

O `servico-agendamentos` consome esse evento e atualiza a projeção de status para
`CANCELADO_POR_CONFLITO`.

Consulta de status observável:

```bash
curl http://localhost:8080/agendamentos/AG-EXEMPLO-001/status
```

Resposta:

```json
{"agendamentoId":"AG-EXEMPLO-001","status":"CANCELADO_POR_CONFLITO"}
```

Falha da própria compensação usa o mesmo padrão de resiliência (retry 1s/2s/4s e DLQ própria):

- `salao.agendamento-cancelado-por-conflito.dlq.servico-agendamentos-compensacao`
