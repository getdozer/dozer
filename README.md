<div align="center">
    <a target="_blank" href="https://getdozer.io/">
        <br><img src="https://getdozer.io/assets/logo-green.svg" width=40%><br>
    </a>
</div>

<h3 align="center">
     Data APIs, done right!
</h3>
<p align="center">
     ⚡️ Open-source platform to build, publish and manage blazing-fast real-time data APIs in minutes ⚡️
</p>

<p align="center">
  <a href="https://github.com/getdozer/dozer/actions/workflows/dozer.yaml" target="_blank"><img src="https://github.com/getdozer/dozer/actions/workflows/dozer.yaml/badge.svg" alt="CI"></a>
  <a href="https://coveralls.io/github/getdozer/dozer?branch=main" target="_blank"><img src="https://coveralls.io/repos/github/getdozer/dozer/badge.svg?branch=main&t=kZMYaV&style=flat" alt="Coverage Status"></a>
  <a href="https://getdozer.io/docs/dozer" target="_blank"><img src="https://img.shields.io/badge/doc-reference-green" alt="Docs"></a>
  <a href="https://discord.com/invite/3eWXBgJaEQ" target="_blank"><img src="https://img.shields.io/badge/join-on%20discord-primary" alt="Join on Discord"></a>
  <a href="https://github.com/getdozer/dozer/blob/main/LICENSE.txt" target="_blank"><img src="https://img.shields.io/badge/license-ELv2-informational" alt="License"></a>

</p>

[//]: # (  <a href="https://github.com/getdozer/dozer/actions/workflows/dozer.yaml" target="_blank"><img src="https://img.shields.io/github/workflow/status/getdozer/dozer/Dozer%20CI?style=flat" alt="build"></a>)

<br>

## Quick Start
Refer to our [getting started](https://getdozer.io/docs/category/getting-started) and [examples section](https://getdozer.io/docs/category/getting-started) for more details.
#### 1) End-to-end hypercharge postgres example
```bash
docker-compose -f examples/1_hypercharge_postgres/docker-compose.yml up
```
#### 2) Real time Ethereum stats example
Get a websocket url from one of the hosted eth node providers such as [Infura](https://www.infura.io/product/ethereum) and initialize the env variable `ETH_WSS_URL`.
```bash
# Intialize ETH Web Socker Url
export ETH_WSS_URL=<WSS_URL>

docker-compose -f examples/2_eth_stats_sample/docker-compose.yml up
```

## Running Dozer CLI
To view list of commands
```bash
docker run -it \
  public.ecr.aws/k7k6x1d4/dozer \
  dozer -h
```

Run dozer as a single process with a local `dozer-config.yaml`
```bash
# `dozer-config.yaml` should be in your current directory.
docker run -it \
  -v "$PWD":/usr/dozer \
  -p 8080:8080 \
  -p 50051:50051 \
  public.ecr.aws/k7k6x1d4/dozer \
  dozer
```

## Releases
We release dozer typically every 2 weeks and is available on our [releases page](https://github.com/getdozer/dozer/releases/latest). Currently we publish a binary for Ubuntu 20.04 and also as a docker container.


Please visit our [issues section](https://github.com/getdozer/dozer/issues) if you are having any trouble running the project.


##  Architecture

![Architecture](https://getdozer.io/assets/images/dozer-binary-e14a8fddd51aa608afe694245eb78271.svg)

- Create **blazing fast** end to end APIs in minutes with a simple configuration.
- Build and rapidly iterate on customer facing data apps.
- Extend Dozer with **custom connectors, operators and Api transformations** using **WASM**.
- Built in **Rust** with performance and extensibility in mind.

Read more about [Dozer here](https://getdozer.io/docs/dozer)
<br>

## Features

- **Connect your sources**
    - Import real time data from Postgres as CDC, Snowflake Table Stream etc.
    - Create your own connector using Rust
    - Automatic schema evolution and validation
- **Transform in REAL-TIME**
    - Use SQL to perform joins, aggregations and filter operations in real time across sources.
    - Use it like an ORM; Map relational data to object entities using Dozer SQL extensions
    - Build custom functions for aggregation, selection etc. using WASM
- **Optimize for serving**
    - Define indices with a simple configuration
    - Support for multiple indices such as Inverted, Full Text, Compound, Geo (Coming soon!) etc.
    - Apply filter and sort operations on cached data
    - Support for Push and Pull queries
- **Publish blazing fast APIs**
    - gRPC and REST APIs automatically generated
    - Protobuf an Open API documentation
    - TypeSafe APIs
    - Realtime Streaming

<br>

## Contributing
Please refer to [Contributing](https://getdozer.io/docs/contributing/overview) for more details.
