### Dozer Workspace [![CI](https://github.com/getdozer/dozer/actions/workflows/dozer.yaml/badge.svg)](https://github.com/getdozer/dozer/actions/workflows/dozer.yaml)

This repository follows a `cargo workspace` structure with several packages. 
```
dozer
|
|-- dozer-ingestion       # Ingestion & Connectors
|-- dozer-storage         # Initial checkpointing of data from ingestion
|-- dozer-api             # APIs to be consumed by clients 
|-- dozer-types          # Library with shared utilities
|-- dozer-pipeline         # Library with shared utilities
```

1) Adding a new package as a service
```
cargo new --vcs none dozer-storage
```

2) Adding a new package as a library
```
cargo new --vcs none --lib dozer-types
```

### Running
Run a specific service with `-p` flag. 
Note: If you have multiple binaries generated,  you can use `--bin` flag.

```
cargo run -p dozer-ingestion
```



### References 
https://doc.rust-lang.org/cargo/reference/workspaces.html
https://www.youtube.com/watch?v=S3c7NRS698A
