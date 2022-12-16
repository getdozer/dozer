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

### gRPC & REST API

#### gRPC API

```yaml
api:
  grpc:
    port: 50051
    url: "[::0]"
    cors: true
    web: true
```

Use server reflection as a source of the gRPC API.

<div align="center">
    <img src="https://drive.google.com/uc?export=view&id=1zcYcUMY7KGJy8MxkZM9noF_MAp5jLPzZ" width=70%">
</div>

`[::0]50051` is from yaml configuration.

<div align="center">
    <img src="https://drive.google.com/uc?export=view&id=11tji0bhcLei7V-SiSQgY6CfMX9r6q_2C" width=70%">
    <img src="https://drive.google.com/uc?export=view&id=1LTayAdpDGWHkJGQ-R9rGDWwOmvctwtBY" width=70%">
</div>

#### REST API

```yaml
api:
  rest:
    port: 8080
    url: "[::0]"
    cors: true

...

endpoints:
  - id: null
    name: stocks
    path: /stocks
    sql: select id, ticker, date, open, high, low, close, adj_close, volume from stocks where 1=1;
    index:
      primary_key:
        - id
  - id: null
    name: stocks_meta
    path: /stocks-meta
    sql: select symbol, security_name from stocks_meta where 1=1;
    index:
      primary_key:
        - symbol
```

Endpoint details are coming from above yaml for REST APIs.

`/stocks`

<div align="center">
    <img src="https://drive.google.com/uc?export=view&id=1p0kbfJbPFAt5ibV9GFJoEdLZa-DArXwT" width=70%">
</div>

`/stocks-meta`

<div align="center">
    <img src="https://drive.google.com/uc?export=view&id=1sAugL5gVxf_5UvJh1H8uRQ7EV-6yvMK2" width=70%">
</div>
