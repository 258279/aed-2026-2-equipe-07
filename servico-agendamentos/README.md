# servico-agendamentos

Instruções para execução isolada (desenvolvimento/integracão local)

Requisitos:

- Docker
- Docker Compose

Subir o serviço com Kafka local (Zookeeper + Kafka incluídos no compose):

```bash
cd servico-agendamentos
docker compose -f docker-compose.dev.yml up --build
```

Variáveis de ambiente suportadas (via `docker compose` ou `environment`):

- `SPRING_KAFKA_BOOTSTRAP_SERVERS` — endereço do broker (ex.: `kafka:9092`)

Portas expostas:

- `8080`

Observações:

- O `docker-compose.dev.yml` sobe Zookeeper e Kafka automaticamente para testes locais.
- Para integração completa com os outros serviços, use o `docker-compose.yml` na raiz do repositório.
