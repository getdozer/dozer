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

[//]: # (Badges for md)
[//]: # (Reference: https://shields.io/)
[//]: # ([![GitHub stars]&#40;https://img.shields.io/github/stars/getdozer/dozer?style=social&label=Star&maxAge=2592000&#41;]&#40;https://gitHub.com/getdozer/dozer/stargazers/&#41;)
[//]: # ([![GitHub Workflow Status]&#40;https://img.shields.io/github/workflow/status/getdozer/dozer/Dozer%20CI?style=flat&#41;]&#40;https://github.com/getdozer/dozer/actions/workflows/dozer.yaml&#41;)
[//]: # ([![CI]&#40;https://github.com/getdozer/dozer/actions/workflows/dozer.yaml/badge.svg&#41;]&#40;https://github.com/getdozer/dozer/actions/workflows/dozer.yaml&#41;)
[//]: # ([![Coverage Status]&#40;https://coveralls.io/repos/github/getdozer/dozer/badge.svg?branch=main&t=kZMYaV&kill_cache=1&#41;]&#40;https://coveralls.io/github/getdozer/dozer?branch=main&#41;)
[//]: # ([![Doc reference]&#40;https://img.shields.io/badge/doc-reference-green?style=flat&#41;]&#40;&#41;)
[//]: # ([![Join on Discord]&#40;https://img.shields.io/badge/join-on%20discord-primary?style=flat&#41;]&#40;&#41;)
[//]: # ([![License]&#40;https://img.shields.io/badge/license-ELv2-informational?style=flat&#41;]&#40;https://github.com/getdozer/dozer/blob/main/LICENSE.txt&#41;)

[//]: # (Badges for html)
<p align="center">
  <!-- <a href="https://gitHub.com/getdozer/dozer/stargazers/" target="_blank"><img src="https://img.shields.io/github/stars/getdozer/dozer?style=social&label=Star&maxAge=2592000" alt="stars"></a> -->
  <a href="https://github.com/getdozer/dozer/actions/workflows/dozer.yaml" target="_blank"><img src="https://github.com/getdozer/dozer/actions/workflows/dozer.yaml/badge.svg" alt="CI"></a>
  <a href="https://coveralls.io/github/getdozer/dozer?branch=main" target="_blank"><img src="https://coveralls.io/repos/github/getdozer/dozer/badge.svg?branch=main&t=kZMYaV&style=flat" alt="Coverage Status"></a>
  <!-- <a><img src="https://img.shields.io/badge/doc-reference-green" alt="Doc reference"></a> -->
  <!-- <a><img src="https://img.shields.io/badge/join-on%20discord-primary" alt="Join on Discord"></a> -->
  <a href="https://github.com/getdozer/dozer/main/LICENSE" target="_blank"><img src="https://img.shields.io/badge/license-ELv2-informational" alt="License"></a>
</p>

[//]: # (  <a href="https://github.com/getdozer/dozer/actions/workflows/dozer.yaml" target="_blank"><img src="https://img.shields.io/github/workflow/status/getdozer/dozer/Dozer%20CI?style=flat" alt="build"></a>)

<br>

## Quick Start

#### Using docker
```bash
docker-compose -f examples/1_hypercharge_postgres/docker-compose.yml up
```
Check out our latest release details [here](https://github.com/getdozer/dozer/releases/latest).

<br>

## Why use Dozer?

- Create **blazing fast** end to end APIs in minutes with a simple configuration.
- Build and rapidly iterate on customer facing data apps.
- Extend Dozer with **custom connectors, operators and Api transformations** using **WASM**.
- Built in **Rust** with performance and extensibility in mind.

<br>

## End-to-end Examples

### What can you build with Dozer?

1. Hypercharge your **Postgres** by offloading read APIs to `Dozer`
2. **Real time ML predictions** deployed as APIs from **Snowflake**
3. **Real time Ethereum Stats** published as a grafana dashboard

[//]: # (### Screenshots)

Check out this module for above [end-to-end examples](./examples/README.md).


[//]: # (## Architecture)

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

### Contributors / Developers

#### Build Dependencies

- [`Rust`](https://rustup.rs)
- [`protoc`](https://github.com/protocolbuffers/protobuf/releases) latest release on your `PATH`
- `sqlite3` (`sudo apt install libsqlite3-dev` on Ubuntu)
- `openssl` (brew install pkg-config openssl on MacOS)

Please refeer to this [module](./dozer-ingestion/tests) on how to **test/build/implement a new connector**.

