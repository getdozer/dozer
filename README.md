### Dozer [![CI](https://github.com/getdozer/dozer/actions/workflows/dozer.yaml/badge.svg)](https://github.com/getdozer/dozer/actions/workflows/dozer.yaml) [![Coverage Status](https://coveralls.io/repos/github/getdozer/dozer/badge.svg?branch=main&t=kZMYaV&kill_cache=1)](https://coveralls.io/github/getdozer/dozer?branch=main)

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
|-- tests                 # End to end test cases & Samples
```


### Samples
[Samples](./tests/README.md) can be found under tests folder. 
[End to End Dozer Sample](./tests/simple_e2e_example/README.md)
[Postgres as an Iterator](./tests/connectors/postgres_as_iterator/README.md)

#### Running a sample
Each of the samples have a `docker-compose.yaml` file. 

Using docker
```
docker-compose up
```

Using binary
```
dozer run
dozer run -c dozer-config.yaml
```


## Local development

### Build Dependencies

- [`Rust`](https://rustup.rs)
- [`protoc`](https://github.com/protocolbuffers/protobuf/releases) latest release on your `PATH`
- `sqlite3` (`sudo apt install libsqlite3-dev` on Ubuntu)
- `openssl` (brew install pkg-config openssl on MacOS)

### Test Dependencies

- `wget` (`brew install wget` on MacOS)

### Local development

By default `config` file is loaded from `dozer-config.yaml`
```
# Initialize config
cp config/sample/dozer-config.sample.yaml dozer-config.yaml

# Run
cargo run 
```
or with a config file
```
cargo run -c dozer-config.sample.yaml
```

### Running individual modules
dozer-api are instantiated part of `dozer` (short for `dozer-orchestrator`).

They can be run separately for local testing using `--bin` flags.
Individual 
```
cargo run --bin dozer
cargo run --bin dozer-api
```