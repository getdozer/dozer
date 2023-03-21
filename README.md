<div align="center">
    <a target="_blank" href="https://getdozer.io/">
        <br><img src="https://dozer-assets.s3.ap-southeast-1.amazonaws.com/logo-blue.svg" width=40%><br>
    </a>
</div>
<p align="center">
    <br />
    <b>
    Connect any data source, combine them in real-time and instantly get low-latency data APIs.<br>
    ⚡ All with just a simple configuration! ⚡️
    </b>
</p>


<br />

<p align="center">
  <a href="https://github.com/getdozer/dozer/actions/workflows/dozer.yaml" target="_blank"><img src="https://github.com/getdozer/dozer/actions/workflows/dozer.yaml/badge.svg" alt="CI"></a>
  <a href="https://coveralls.io/github/getdozer/dozer?branch=main" target="_blank"><img src="https://coveralls.io/repos/github/getdozer/dozer/badge.svg?branch=main&t=kZMYaV&style=flat" alt="Coverage Status"></a>
  <a href="https://getdozer.io/docs/dozer" target="_blank"><img src="https://img.shields.io/badge/doc-reference-green" alt="Docs"></a>
  <a href="https://discord.com/invite/3eWXBgJaEQ" target="_blank"><img src="https://img.shields.io/badge/join-on%20discord-primary" alt="Join on Discord"></a>
  <a href="https://github.com/getdozer/dozer/blob/main/LICENSE.txt" target="_blank"><img src="https://img.shields.io/badge/license-ELv2-informational" alt="License"></a>
</p>

## Overview

Dozer makes it easy to build low-latency data APIs (gRPC and REST) from any data source. Data is transformed on the fly using Dozer's reactive SQL engine  and stored in a high-performance cache to offer the best possible experience. Dozer is useful for quickly building data products.

![Architecture](./images/dozer.png)


## Video Tour

<iframe src="https://share.descript.com/embed/bIzWSb60R5n" width="640" height="360" frameborder="0" allowfullscreen></iframe>

## Quick Start

Follow the instruction below to install Dozer on your machine and run a quick sample using the [NY Taxi Dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

### Installation

**MacOS Monterey (12) and above**

```bash
brew tap getdozer/dozer && brew install dozer
```

**Ubuntu 20.04**

```bash
curl -sLO https://github.com/getdozer/dozer/releases/download/latest/dozer_linux_x86_64.deb && sudo dpkg -i dozer_linux_x86_64.deb 
```

**Build from source**

```bash
cargo install --path dozer-orchestrator --locked
```

### Run it

**Download sample configuration and data**

Create a new empty directory and run the commands below. This will download a [sample configuration file](https://github.com/getdozer/dozer-samples/blob/main/connectors/local-storage/dozer-config.yaml) and a sample [NY Taxi Dataset file](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

```bash
curl -o dozer-config.yaml https://raw.githubusercontent.com/getdozer/dozer-samples/main/connectors/local-storage/dozer-config.yaml
curl --create-dirs -o data/trips/fhvhv_tripdata_2022-01.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2022-01.parquet
```

**Run Dozer binary**

```bash
dozer -c dozer-config.yaml
```

Dozer will start processing the data and populating the cache. You can see a progress of the execution from the console.

**Query the APIs**

When some data is loaded, you can query the cache using gRPC or REST

```bash
# gRPC
grpcurl -d '{"query": "{\"$limit\": 1}"}' -plaintext localhost:50051 dozer.generated.trips_cache.TripsCaches/query

# REST
curl -X POST  http://localhost:8080/trips/query --header 'Content-Type: application/json' --data-raw '{"$limit":3}'
```

Alternatively, you can use [Postman](https://www.postman.com/) to discover gRPC endpoints through gRPC reflection

![postman query](images/postman.png)

Read more about Dozer [here](https://getdozer.io/docs/dozer). And remember to star 🌟 our repo to support us!


## Client Libraries

| Library                                                  | Language                                              | License |
| -------------------------------------------------------- | ----------------------------------------------------- | ------- |
| [dozer-python](https://github.com/getdozer/dozer-python) | Dozer Client library for Python                       | MIT     |
| [dozer-js](https://github.com/getdozer/dozer-js)         | Dozer Client library for Javascript                   | MIT     |
| [dozer-react](https://github.com/getdozer/dozer-react)   | Dozer Client library for React with easy to use hooks | MIT     |

<br>


[**Python**](https://github.com/getdozer/dozer-python)
```python
from dozer.api import ApiClient
api_client = ApiClient('trips')
api_client.query()
```

[**Javascript**](https://github.com/getdozer/dozer-js)
```js
import { ApiClient } from "@getdozer/dozer-js";

const flightsClient = new ApiClient('flights');
flightsClient.count().then(count => {
    console.log(count);
});
```

[**React**](https://github.com/getdozer/dozer-react)
```js
import { useCount } from "@getdozer/dozer-react";
const AirportComponent = () => {
    const [count] = useCount('trips');
    <div> Trips: {count} </div>
}
```

## Samples

Check out Dozer's [samples repository](https://github.com/getdozer/dozer-samples) for more comprehensive examples and use case scenarios. 

| Type             | Sample                                                                                                                     | Notes                                                                        |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| Connectors       | [Postgres](https://github.com/getdozer/dozer-samples/tree/main/connectors/postgres)                                        | Load data using Postgres CDC                                                 |
|                  | [Local Storage](https://github.com/getdozer/dozer-samples/tree/main/connectors/local-storage)                              | Load data from local files                                                   |
|                  | Snowflake (Coming soon)                                                                                                    | Load data using Snowflake table streams                                      |
| SQL              | [Using JOINs](https://github.com/getdozer/dozer-samples/tree/main/sql/join)                                                | Dozer APIs over multiple sources using JOIN                                  |
|                  | [Using Aggregations](https://github.com/getdozer/dozer-samples/tree/main/sql/aggregations)                                 | How to aggregate using Dozer                                                 |
|                  | [Using Window Functions](https://github.com/getdozer/dozer-samples/tree/main/sql/window-functions)                         | Use `Hop` and `Tumble` Windows                                               |
| Use Cases        | [Flight Microservices](https://github.com/getdozer/dozer-samples/tree/main/usecases/pg-flights)                            | Build APIs over multiple microservices.                                      |
|                  | Use Dozer to Instrument (Coming soon)                                                                                      | Combine Log data to get real time insights                                   |
|                  | Real Time Model Scoring (Coming soon)                                                                                      | Deploy trained models to get real time insights as APIs                      |
| Client Libraries | Dozer React Starter (Coming soon)                                                                                          | Instantly start building real time views using Dozer and React               |
|                  | [Ingest Polars/Pandas Dataframes](https://github.com/getdozer/dozer-samples/tree/main/client-samples/ingest-python-sample) | Instantly ingest Polars/Pandas dataframes using Arrow format and deploy APIs |
| Authorization    | Dozer Authorziation (Coming soon)                                                                                          | How to apply JWT Auth on Dozer                                               |

# Connectors

Refer to the full list of connectors and example configurations [here](https://getdozer.io/docs/configuration/connectors).

| Connector                          |   Status    | Type           |  Schema Mapping   | Frequency | Implemented Via |
| :--------------------------------- | :---------: | :------------- | :---------------: | :-------- | :-------------- |
| Postgres                           | Available ✅ | Relational     |      Source       | Real Time | Direct          |
| Snowflake                          | Available ✅ | Data Warehouse |      Source       | Polling   | Direct          |
| Local Files (CSV, Parquet)         | Available ✅ | Object Storage |      Source       | Polling   | Data Fusion     |
| Delta Lake                         |    Alpha    | Data Warehouse |      Source       | Polling   | Direct          |
| AWS S3 (CSV, Parquet)              |    Alpha    | Object Storage |      Source       | Polling   | Data Fusion     |
| Google Cloud Storage(CSV, Parquet) |    Alpha    | Object Storage |      Source       | Polling   | Data Fusion     |
| Ethereum                           | Available ✅ | Blockchain     | Logs/Contract ABI | Real Time | Direct          |
| MySQL                              | In Roadmap  | Relational     |      Source       | Real Time | Debezium        |
| Kafka                              | In Roadmap  | Stream         |  Schema Registry  | Real Time | Debezium        |
| Google Sheets                      | In Roadmap  | Applications   |      Source       |           |                 |
| Excel                              | In Roadmap  | Applications   |      Source       |           |                 |
| Airtable                           | In Roadmap  | Applications   |      Source       |           |                 |


## Releases

We release Dozer typically every 2 weeks and is available on our [releases page](https://github.com/getdozer/dozer/releases/latest). Currently, we publish binaries for Ubuntu 20.04, Apple(Intel) and Apple(Silicon).


Please visit our [issues section](https://github.com/getdozer/dozer/issues) if you are having any trouble running the project.

## Contributing

Please refer to [Contributing](https://getdozer.io/docs/contributing/overview) for more details.
