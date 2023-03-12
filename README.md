<div align="center">
    <a target="_blank" href="https://getdozer.io/">
        <br><img src="https://dozer-assets.s3.ap-southeast-1.amazonaws.com/logo-blue.svg" width=40%><br>
    </a>
</div>
<p align="center">
    <br />
    <b>
    Connect any data source, combine them in real-time and instantly get low-latency APIs.<br>
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



## Quick Start

Follow the instruction below to install Dozer on your machine and run a quick sample using the [NY Taxi Dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

### Installation

#### MacOS Monterey (12) and above

```bash
brew tap getdozer/dozer && brew install dozer
```

#### Ubuntu 20.04

```bash
curl -sLO https://github.com/getdozer/dozer/releases/download/latest/dozer_linux_x86_64.deb && sudo dpkg -i dozer_linux_x86_64.deb 
```

#### Build from source

```bash
cargo install --path dozer-orchestrator --locked
```

### Run it

#### Download sample configuration and data

Download a sample configuration file

```bash
curl https://raw.githubusercontent.com/getdozer/dozer-samples/main/local-storage/dozer-config.yaml
curl --create-dirs -o data/trips/fhvhv_tripdata_2022-01.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2022-01.parquet
```

#### Run Dozer binary

```bash
dozer -c dozer-config.yaml
```

Dozer will start processing the data and populating the cache. You can see a progress of the execution from teh console.

### Query the APIs

The easiest way to query Dozer cache is using gRPC APIs is to use [Postman](https://www.postman.com/). Point your Postman gRPC address to `localhost:50051` and start querying the Dozer cache.

![postman query](images/postman.png)

<div align="center">
    <b>Please star ⭐️ the repo if you want us to continue developing and improving Dozer! 💡</b>
</div>


## What can you do with Dozer

- Create **blazing fast** end to end APIs in minutes with a simple configuration.
- Build and rapidly iterate on customer facing data apps.
- Transform your data in real-time using standard SQL.
- Cache your data and get search and filter functionality out of the box.
- Extend Dozer with **custom connectors, operators and Api transformations** using **WASM**.
- Built with **Rust** with performance and extensibility in mind.
- OpenAPI documentation and protobuf-based data contracts are auto-generated.

Read more about [Dozer here](https://getdozer.io/docs/dozer)

Detailed [Architecture can be found here](https:///getdozer.io/docs/dozer/architecture).

## Features

**Plug & Play**

Dozer instantly generates fully indexed gRPC and REST APIs automatically. All you need is to configure a YAML file with your data source configuration and the APIs you want to deploy. As simple as that. There is no need to write any additional code saving several developer hours. Dozer is very much customizable. You can refer to your [Contributing](https://getdozer.io/docs/contributing/overview) section for more information on building more connectors as well as transformations.

**Connect to all sources**

Dozer doesn't make a distinction between types of data sources. Developers can get a seamless experience building products with application databases such as Postgres and MySQL, data warehouses such as SnowFlake and cloud storage such as S3 and Deltalake. Dozer can also consume real-time events and Ethereum data.

**Combine data from multiple sources**

Dozer can in real-time join data coming from multiple data sources powering advanced use cases such as customer personalization, real-time analytics etc. This can be done using the standard JOIN operator in SQL.

**APIs & Real-time queries**

At the heart of the implementation, Dozer has a streaming data pipeline that works on CDC across all stores. As new data flows in, Dozer incrementally computes aggregations and joins, and offers a far superior query experience than a traditional database for these scenarios.

Data is stored in a cache built on LMDB (Lightning Memory-Mapped Database) and secondary indexes for single columns are automatically built. This gives users instant queryable APIs with operations such as filter, sort, and order_by functionality.

**Scaling**

Dozer can be run as a single process for simple applications or can be run in a distributed fashion where writing and reading are de-coupled. This is a cost-effective approach where reading has a very low overhead and can be scaled on demand.

**Authorization**

Dozer offers authorization functionality through JWT tokens. Refer to [API Security](https://getdozer.io/docs/reference/api/security) for more details.

## Comparision

Dozer takes an opinionated and horizontal approach that cuts across different categories. In Dozer, you would find modules and functionalities comparable to streaming databases, caches, search engines and API generation tools.

You can find comparision [documented here](https://getdozer.io/docs/dozer/comparision)

## Releases

We release Dozer typically every 2 weeks and is available on our [releases page](https://github.com/getdozer/dozer/releases/latest). Currently, we publish a binary for Ubuntu 20.04 and also as a docker container.

Please visit our [issues section](https://github.com/getdozer/dozer/issues) if you are having any trouble running the project.

<br>

## Contributing

Please refer to [Contributing](https://getdozer.io/docs/contributing/overview) for more details.
