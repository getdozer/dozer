## Dozer [![CI](https://github.com/getdozer/dozer/actions/workflows/dozer.yaml/badge.svg)](https://github.com/getdozer/dozer/actions/workflows/dozer.yaml) [![Coverage Status](https://coveralls.io/repos/github/getdozer/dozer/badge.svg?t=kZMYaV)](https://coveralls.io/github/getdozer/dozer)

[Dozer](https://getdozer.io/) is a no-code data infrastructure platform that allows you to deploy highly scalable APIs from your data warehouse with minimal configuration.
Start building data apps without investing in a complex data engineering setup.<br>

This repository follows a `cargo workspace` structure with several packages. 
```
dozer
|
|-- dozer-admin           # gRPC APIs for Dozer Admin UI
|-- dozer-ingestion       # Ingestion & Connectors
|-- dozer-api             # Data APIs in REST & gRPC
|-- dozer-cache           # Cache Implementation
|-- dozer-core            # Dozer Libaries such as dag, state_store
|-- dozer-orchestrator    # Dozer Orchestrator & Cli
|-- dozer-types           # Dozer Types
|-- tests                 # End to end test cases & Examples
```

### Examples
[Examples](./tests/README.md) can be found under `tests` folder.<br>
[End to End Dozer Example](./tests/simple_e2e_example/README.md)<br>
[Postgres as an Iterator](./tests/connectors/postgres_as_iterator/README.md)

Each of example has a `docker-compose.yaml` file to be utilized.

#### Using docker
```
docker-compose up
```

#### Using binary
Using default `config` file loaded from `dozer-config.yaml`
```
dozer run
```
Using custom `config` file
```
dozer run -- -c <custom-config-file-path>
```


### Local Development

#### Pre-requisites

- [`Rust`](https://rustup.rs)
- [`protoc`](https://github.com/protocolbuffers/protobuf/releases) latest release on your `PATH`
- `sqlite3` (`sudo apt install libsqlite3-dev` on Ubuntu)

#### Local Development

By default `config` file is loaded from `dozer-config.yaml`
```
# Initialize config
cp config/dozer-config.sample.yaml dozer-config.yaml

# Run
cargo run 
```
or with a config file
```
cargo run -- -c dozer-config.sample.yaml
```

#### Logging

```
# Initialize `log4rs.yaml`
cp config/log4rs.sample.yaml log4rs.yaml
```

#### Running individual modules
dozer-api are instantiated part of `dozer` (short for `dozer-orchestrator`).

They can be run separately for local testing using `--bin` flags.
```
# Running `dozer-orchestrator` module
cargo run -- --bin dozer

# Running `dozer-api` module
cargo run -- --bin dozer-api
```