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

<details>
<summary><h3>gRPC</h3></summary>
Example Config for gRPC

```yaml
grpc:
    port: 50051
    url: "[::0]"
    cors: true
    web: true
```

Use server reflection as a source of the gRPC API.

<div align="center">
    <img src="https://drive.google.com/uc?export=view&id=1zcYcUMY7KGJy8MxkZM9noF_MAp5jLPzZ" width=60%">
</div>

`[::0]50051` is from yaml configuration.

<div align="center">
    <img src="https://drive.google.com/uc?export=view&id=11tji0bhcLei7V-SiSQgY6CfMX9r6q_2C" width=60%">
</div>

</details>

