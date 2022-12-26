## End-to-end Hypercharge Postgres Example - Stock Price Dataset

Features used:
- Initialize project from [yaml](./docker-compose.yml) with docker
- Setup postgresql source with pre-processed stock price dataset [init_stocks.sql](./scripts/init_stocks.sql)
- Ingestion from postgresql source
- SQL execution and gRPC & REST API endpoints creation from [yaml](./dozer-config.yaml)

[//]: # (- Creation of embeddable React widget)

### Run

```bash
# For Apple silicons pre-setting
# export DOCKER_DEFAULT_PLATFORM=linux/amd64

docker-compose up --build
```

<br>

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

Use server reflection as a source of the gRPC API. `[::0]50051` is from yaml configuration.

<details>
<summary>Screenshots</summary>
<div align="center">
    <img src="https://drive.google.com/uc?export=view&id=1zcYcUMY7KGJy8MxkZM9noF_MAp5jLPzZ" width=70%">
</div>

<div align="center">
    <img src="https://drive.google.com/uc?export=view&id=11tji0bhcLei7V-SiSQgY6CfMX9r6q_2C" width=70%">
    <img src="https://drive.google.com/uc?export=view&id=18qScvOY8q9UM0zeu5DgViwUN37y2cbIy" width=70%">
</div>
</details>

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



<details>
<summary>Screenshots</summary>

`/stocks`

<div align="center">
    <img src="https://drive.google.com/uc?export=view&id=1p0kbfJbPFAt5ibV9GFJoEdLZa-DArXwT" width=70%">
</div>

`/stocks-meta`

<div align="center">
    <img src="https://drive.google.com/uc?export=view&id=1sAugL5gVxf_5UvJh1H8uRQ7EV-6yvMK2" width=70%">
</div>
</details>

<br>

## Kaggle Stock Price Datasets

This test case is based on Kaggle's Stock Price Datasets and pre-processed to be used.
Check out [kaggle](https://www.kaggle.com/datasets/jacksoncrow/stock-market-dataset) for more details on this open source dataset.

#### Dataset Pre-processing
```bash
# Remove headers from all tables
sed -i '' 1d *.csv

# Combine tables into one csv
INDEX=0
for f in *.csv
do
    if [[ ${INDEX} -le 2 ]]; then
        INDEX=${INDEX}+1
        sed -e "s/^/${f%.*},/" $f >> stock_price.csv
    fi
done
```
