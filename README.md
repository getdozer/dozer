## Overview

Dozer is a **real time data movement tool leveraging CDC from various sources to multiple sinks.**

Dozer is magnitudes of times faster than Debezium+Kafka and natively supports stateless transformations. 
Primarily used for moving data into warehouses. In our own application, we move data to **Clickhouse** and build data APIs and integration with LLMs. 

## How to use it
Dozer runs with a single configuration file like the following:
```yaml
app_name: dozer-bench
version: 1
connections:
  - name: pg_1
    config: !Postgres
      user: user
      password: postgres
      host: localhost
      port: 5432
      database: customers
sinks:
  - name: customers
    config: !Dummy
      table_name: customers
```

Full documentation can be found [here](https://github.com/getdozer/dozer/blob/main/dozer-types/src/models/config.rs#L15)


## Supported Sources

| Connector            | Extraction | Resuming | Enterprise          |
| -------------------- | ---------- | -------- | ------------------- |
| Postgres             | ✅          | ✅        | ✅                   |
| MySQL                | ✅          | ✅        | ✅                   |
| Snowflake            | ✅          | ✅        | ✅                   |
| Kafka                | ✅          | 🚧        | ✅                   |
| MongoDB              | ✅          | 🎯        | ✅                   |
| Amazon S3            | ✅          | 🎯        | ✅                   |
| Google Cloud Storage | ✅          | 🎯        | ✅                   |
| **Oracle             | ✅          | ✅        | **Enterprise Only** |
| **Aerospike          | ✅          | ✅        | **Enterprise Only** |


## Supported Sinks
| Database   | Connectivity | Enterprise          |
| ---------- | ------------ | ------------------- |
| Clickhouse | ✅            |                     |
| Postgres   | ✅            |                     |
| MySQL      | ✅            |                     |
| Big Query  | ✅            |                     |
| Oracle     | ✅            | **Enterprise Only** |
| Aerospike  | ✅            | **Enterprise Only** |