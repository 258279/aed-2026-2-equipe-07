# servico-ocupacao

Instruções para execução isolada (desenvolvimento/integracão local)

Requisitos:

- Docker
- Docker Compose

Subir o serviço com Kafka e Postgres locais:

```bash
cd servico-ocupacao
docker compose -f docker-compose.dev.yml up --build
```

Variáveis de ambiente suportadas:

- `SPRING_KAFKA_BOOTSTRAP_SERVERS` — endereço do broker (ex.: `kafka:9092`)
- `SPRING_DATASOURCE_URL` — conexão JDBC (ex.: `jdbc:postgresql://postgres:5432/ocupacao`)
- `SPRING_DATASOURCE_USERNAME` — usuário do Postgres (default `user` no compose)
- `SPRING_DATASOURCE_PASSWORD` — senha do Postgres (default `pass` no compose)
- `SERVER_PORT` — porta do serviço (ex.: `8081`)

Portas expostas:

- `8081`

Observações:

- O `docker/postgres-init/init.sql` (raiz do repositório) cria as tabelas necessárias para idempotência e projeção.
- Para integração completa com os outros serviços, use o `docker-compose.yml` na raiz do repositório.
