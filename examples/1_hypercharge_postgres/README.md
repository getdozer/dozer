## End-to-end Hypercharge Postgres Example - Stock Price Dataset

Features used:
- Initialize project from [yaml](./docker-compose.yml) with docker
- Setup postgresql source with [pre-processed stock price dataset](./data/README.md) [init_stocks.sql](./scripts/init_stocks.sql)
- Ingestion from postgresql source
- SQL execution and gRPC & REST API endpoints creation from [yaml](./dozer-config.yaml)

[//]: # (- Creation of embeddable React widget)

### Run

```bash
docker-compose up --build
```

```bash
cargo run
```

### Postman Integration

#### gRPC
```yaml
grpc:
    port: 50051
    url: "[::0]"
    cors: true
    web: true
```

![gRPC postman screenshot 1](https://drive.google.com/uc?export=view&id=1zcYcUMY7KGJy8MxkZM9noF_MAp5jLPzZ)
![gRPC postman screenshot 2](https://drive.google.com/uc?export=view&id=11tji0bhcLei7V-SiSQgY6CfMX9r6q_2C)
