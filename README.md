### Dozer Workspace [![CI](https://github.com/getdozer/dozer/actions/workflows/dozer.yaml/badge.svg)](https://github.com/getdozer/dozer/actions/workflows/dozer.yaml)

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
|-- dozer-schema          # Schema Registry 
|-- dozer-types           # Dozer Types
|-- tests                 # End to end test cases & Samples
```


### Samples
[Samples](./tests/README.md) can be found under tests folder. 
[End to End Dozer Sample](./tests/simple_e2e_example/README.md)
[Postgres as an Iterator](./tests/connectors/postgres_as_iterator/README.md)

#### Running a sample
Each of the samples have a `docker-compose.yaml` file. 
```
docker-compose up
```

## Local development

### Build Dependencies

- [`Rust`](https://rustup.rs)
- [`protoc`](https://github.com/protocolbuffers/protobuf/releases) latest release on your `PATH`
- `sqlite3` (`sudo apt install libsqlite3-dev` on Ubuntu)

### Local development

By default `config` file is loaded from `dozer-config.yaml`
```
cp dozer-config.sample.yaml dozer-config.yaml
dozer run
```
or with a config file
```
cargo run --bin dozer run -c dozer-config.sample.yaml
```

### Running individual modules
dozer-schema, dozer-api are instantiated part of `dozer` (short for `dozer-orchestrator`).

They can be run separately for local testing using `--bin` flags.
Individual 
```
cargo run --bin dozer-api
cargo run --bin dozer-schema
```