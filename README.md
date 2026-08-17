# aed-2026-2-equipe-07

## Líder:

## Integrantes:

( ) - Diego Cardoso Marques
( ) - Gabriel Yuji Yasuda Cardoso
(258850) - João Vítor Vieira Martins
( ) - Júlio César Fernandes
(258279) - Lucas Gabriel Lisboa Alves
(255180) - William Tavares de Moura

## (O domínio em uma frase)

##

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
