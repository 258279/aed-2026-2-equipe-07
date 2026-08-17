# Aula 02 — Entrega da Equipe 07

Data da validação: 2026-08-16

## O que foi feito nesta etapa
 
- Escolha do domínio do projeto — **agendamento de salão de beleza** — e registro da
  decisão em ADR-002, com os quatro critérios da atividade endereçados e justificados.
- Modelagem do evento `AgendamentoConfirmadoEvent`: imutável, com `eventoId` próprio,
  separado do `agendamentoId`, e datas em ISO-8601.
- Implementação do publisher no `servico-agendamentos`: endpoint HTTP que confirma um
  agendamento e publica o evento no tópico `salao.agendamento-confirmado`, com envelope
  CloudEvents 1.0 completo e chave de partição por `agendamentoId`.
- Implementação do consumidor idempotente no `servico-ocupacao`: deduplicação por
  `eventoId`, efeito de negócio e registro de deduplicação no mesmo commit, ACK após o
  processamento, e tolerância a campos desconhecidos no payload.
- Infraestrutura local via Docker Compose: Zookeeper, Kafka, Postgres e os dois serviços
  da aplicação, com as tabelas `eventos_processados` e `projecao_ocupacao` criadas
  automaticamente na subida via script de inicialização do Postgres.
- Testes automatizados cobrindo publisher, consumidor e o cenário de reentrega do mesmo
  evento três vezes, comprovando efeito único.
- Registro das interações com IA ao longo da semana, incluindo recusas justificadas.

## Onde está cada coisa
 
| O quê | Onde |
|---|---|
| Domínio e decisão de arquitetura | [`docs/adr/ADR-002-dominio-do-projeto.md`](../adr/ADR-002-dominio-do-projeto.md) |
| Registro de uso de IA | [`docs/IA.md`](../IA.md) |
| Infraestrutura (Zookeeper, Kafka, Postgres, serviços) | [`docker-compose.yml`](../../docker-compose.yml) |
| Script de criação das tabelas | [`docker/postgres-init/init.sql`](../../docker/postgres-init/init.sql) |
| Evento e publisher | [`servico-agendamentos/`](../../servico-agendamentos) |
| Consumidor idempotente | [`servico-ocupacao/`](../../servico-ocupacao) |
| Identificação da equipe e como rodar (visão geral) | [`README.md`](../../README.md) |

## Integrantes

- Diego Cardoso Marques
- Gabriel Yuji Yasuda Cardoso
- João Vítor Vieira Martins
- Júlio César Fernandes
- Lucas Gabriel Lisboa Alves
- William Tavares de Moura

## O que foi validado

- Docker Compose funcionando.
- Kafka funcionando.
- Publisher e consumer funcionando juntos.
- Código revisado.
- Padrões da Seção 12 atendidos.
- Checklist da Seção 5 conferido.

## Como executar

#### 0. Subir tudo pela raiz com broker compartilhado

```bash
git clone <url-do-repositorio>
cd aed-2026-2-equipe-07
docker-compose up -d --build
```

Esse caminho sobe o broker compartilhado, o Postgres e os dois serviços no mesmo ambiente. É o fluxo mais próximo da execução integrada do projeto.

#### 1. Subir o publisher com Kafka local

```bash
cd servico-agendamentos
docker-compose -f docker-compose.dev.yml up -d --build
```

#### 2. Subir o banco e o consumer na mesma rede do publisher

```bash
cd servico-ocupacao
docker build -t servico-ocupacao-shared:latest .

NETWORK=$(docker network ls --filter "name=servico-agendamentos" --format "{{.Name}}")

docker run -d \
	--name db-shared \
	--network "$NETWORK" \
	-e POSTGRES_DB=ocupacao \
	-e POSTGRES_USER=user \
	-e POSTGRES_PASSWORD=pass \
	-p 5434:5432 \
	-v /home/gabriel-yuji/dev/aed-2026-2-equipe-07/docker/postgres-init:/docker-entrypoint-initdb.d:ro \
	postgres:15

docker run -d \
	--name servico-ocupacao-shared \
	--network "$NETWORK" \
	-e SPRING_KAFKA_BOOTSTRAP_SERVERS=servico-agendamentos-kafka-1:9092 \
	-e SPRING_DATASOURCE_URL=jdbc:postgresql://db-shared:5432/ocupacao \
	-e SPRING_DATASOURCE_USERNAME=user \
	-e SPRING_DATASOURCE_PASSWORD=pass \
	-e SERVER_PORT=8081 \
	-p 8081:8081 \
	servico-ocupacao-shared:latest
```

#### 3. Publicar um evento de teste

```bash
curl -s http://localhost:8080/agendamentos/confirmacoes \
	-X POST \
	-H "Content-Type: application/json" \
	-d '{
		"eventoId": "EVT-DOC-001",
		"agendamentoId": "AG-DOC-001",
		"profissionalId": "PROF-DOC-001",
		"servicoId": "SERV-DOC-001",
		"inicioEm": "2026-08-22T16:00:00-03:00",
		"prioridade": "URGENTE",
		"ocorridoEm": "2026-08-17T00:15:00-03:00"
	}' -w "\nHTTP %{http_code}\n"
```

### Provas coletadas

#### 1. Docker Compose e containers ativos

```text
servico-ocupacao-shared                       servico-ocupacao-shared:latest   0.0.0.0:8081->8081/tcp   Up
db-shared                                     postgres:15                      0.0.0.0:5433->5432/tcp   Up
servico-agendamentos-servico-agendamentos-1   servico-agendamentos             0.0.0.0:8080->8080/tcp   Up
servico-agendamentos-kafka-1                  confluentinc/cp-kafka:7.7.1      0.0.0.0:9092->9092/tcp   Up
servico-agendamentos-zookeeper-1              confluentinc/cp-zookeeper:7.7.1  0.0.0.0:2181->2181/tcp   Up
```

#### 2. Kafka funcionando

```text
Topic: salao.agendamento-confirmado     TopicId: VjMM2oaXT5Gv4rE7LNQRaw   PartitionCount: 1   ReplicationFactor: 1
				Topic: salao.agendamento-confirmado     Partition: 0     Leader: 1     Replicas: 1     Isr: 1
```

#### 3. Publisher respondendo com HTTP 202

```text
HTTP 202
```

#### 4. Consumer inscrito no tópico

```text
Subscribed to topic(s): salao.agendamento-confirmado
Successfully joined group with generation Generation{generationId=1, memberId='consumer-servico-ocupacao-1-...'
```

#### 5. Evento publicado e consumido com persistência

```text
 evento_id   |       processado_em
--------------+----------------------------
 EVT-DOC-001  | 2026-08-17 00:17:54.8997
 EVT-E2E-PROD | 2026-08-17 00:15:31.058081

 agendamento_id | profissional_id |         inicio_em
----------------+-----------------+---------------------------
 AG-PROD-001    | PROF-PROD-001   | 2026-08-22T16:00:00-03:00
 AG-DOC-001     | PROF-DOC-001    | 2026-08-22T16:00:00-03:00
```

### Código revisado

- Publisher HTTP e publicação Kafka em [servico-agendamentos/src/main/java/br/pucminas/aed/equipe07/agendamentos/controller/AgendamentoController.java](../../servico-agendamentos/src/main/java/br/pucminas/aed/equipe07/agendamentos/controller/AgendamentoController.java) e [servico-agendamentos/src/main/java/br/pucminas/aed/equipe07/agendamentos/service/AgendamentoService.java](../../servico-agendamentos/src/main/java/br/pucminas/aed/equipe07/agendamentos/service/AgendamentoService.java).
- Consumer Kafka com ack manual em [servico-ocupacao/src/main/java/br/pucminas/aed/equipe07/ocupacao/controller/AgendamentoConfirmadoListener.java](../../servico-ocupacao/src/main/java/br/pucminas/aed/equipe07/ocupacao/controller/AgendamentoConfirmadoListener.java).
- Idempotência e projeção em [servico-ocupacao/src/main/java/br/pucminas/aed/equipe07/ocupacao/service/OcupacaoService.java](../../servico-ocupacao/src/main/java/br/pucminas/aed/equipe07/ocupacao/service/OcupacaoService.java) e [servico-ocupacao/src/main/java/br/pucminas/aed/equipe07/ocupacao/service/OcupacaoRepository.java](../../servico-ocupacao/src/main/java/br/pucminas/aed/equipe07/ocupacao/service/OcupacaoRepository.java).

### Revisão do padrão de código

#### Pacotes

- Os pacotes esperados no código atual estão organizados em `controller`, `domain` e `service`.
- No consumer, a persistência ficou em `service` com `OcupacaoRepository`, e não em um pacote separado de infraestrutura.
- Não existe um pacote `events` separado no projeto; os eventos estão representados em `domain`.

#### Nomes das classes

- Os sufixos usados seguem o padrão permitido: `Controller`, `Service`, `Repository`, `Listener` e `Event`.
- Não foram encontrados nomes fora desse padrão como `Helper`, `Util`, `Manager`, `Impl`, `Producer` ou `DTO` nas classes de produção.

#### Domínio independente de infraestrutura

- A busca por `org.springframework` e `org.apache.kafka` dentro de `domain/` não retornou ocorrências.
- Isso indica que o `domain` está sem dependência direta de framework de infraestrutura.

#### `@Transactional`

- A busca por `@Transactional` encontrou uma única ocorrência, em [servico-ocupacao/src/main/java/br/pucminas/aed/equipe07/ocupacao/service/OcupacaoService.java](../../servico-ocupacao/src/main/java/br/pucminas/aed/equipe07/ocupacao/service/OcupacaoService.java).
- Não há `@Transactional` em `Controller` nem em `KafkaListener`.

#### Publisher e consumer independentes

- Os dois serviços possuem seus próprios arquivos Maven: [servico-agendamentos/pom.xml](../../servico-agendamentos/pom.xml) e [servico-ocupacao/pom.xml](../../servico-ocupacao/pom.xml).
- Cada lado declara sua própria classe de evento em `domain`.
- Não foi usado módulo central obrigatório de contrato entre publisher e consumer.

### Padrões da Seção 12 atendidos

- Separação clara entre publisher e consumer.
- Uso de evento de domínio para comunicação entre serviços.
- Publicação em tópico único e explícito.
- Consumo com `@KafkaListener` e confirmação manual de offset.
- Persistência idempotente com tabela de eventos processados.
- Projeção de leitura separada da escrita do evento.

### Checklist da Seção 5 conferido

- [x] Docker Compose sobe os serviços necessários.
- [x] Kafka sobe e expõe o tópico do fluxo.
- [x] Publisher aceita a requisição e retorna 202.
- [x] Consumer se inscreve no tópico.
- [x] Evento publicado chega ao banco do consumer.
- [x] Projeção de ocupação é gravada.

### Observação de reprodução

A validação final foi feita com a topologia funcional: publisher em Docker Compose, Kafka compartilhado na mesma rede e consumer apontando para o mesmo broker e banco. Isso garante que o fluxo publisher -> Kafka -> consumer -> PostgreSQL foi realmente exercitado durante a checagem.
