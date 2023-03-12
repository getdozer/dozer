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

Create a new empty directory and run the commands below. This will download a [sample configuration file](https://github.com/getdozer/dozer-samples/blob/main/local-storage/dozer-config.yaml) and a sample [NY Taxi Dataset file](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

```bash
curl -o dozer-config.yaml https://raw.githubusercontent.com/getdozer/dozer-samples/main/local-storage/dozer-config.yaml
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

### More Samples

Check out Dozer's [samples repository](https://github.com/getdozer/dozer-samples) for more comprehensive examples and use case scenarios. 
