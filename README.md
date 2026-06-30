## Overview

Dozer is a **real time data movement tool leveraging CDC from various sources to multiple sinks.**

Dozer is magnitudes of times faster than Debezium+Kafka and natively supports stateless transformations. 
Primarily used for moving data into warehouses. In our own application, we move data to **Clickhouse** and build data APIs and integration with LLMs. 

## Getting Started

Watch the [Getting Started video](https://youtu.be/pVfyMfNrlD4) for a quick introduction to Dozer, or see the [Community Videos](#community-videos) section below for more tutorials.

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

## Community Videos

Community-contributed videos covering Dozer features, demos, and use cases:

| Title | Author | Description |
| ----- | ------ | ----------- |
| [Getting Started with Dozer](https://youtu.be/pVfyMfNrlD4) | Dozer Team | Official introduction and quickstart demo |

> Want to add your video? Submit a PR adding a row to this table. See [CONTRIBUTING.md](CONTRIBUTING.md) for details.
