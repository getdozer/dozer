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

### Running
Run a specific service with `-p` flag. 
Note: If you have multiple binaries generated,  you can use `--bin` flag.

```
cargo run --bin dozer
```

### Build Dependencies

- [`Rust`](https://rustup.rs)
- [`protoc`](https://github.com/protocolbuffers/protobuf/releases) latest release on your `PATH`
- `sqlite3` (`sudo apt install libsqlite3-dev` on Ubuntu)


### Samples
[Samples](./tests/README.md) can be found under tests folder. 

[End to End Dozer Sample](./tests/simple_e2e_example/README.md)
[Postgres as an Iterator](./tests/connectors/postgres_as_iterator/README.md)