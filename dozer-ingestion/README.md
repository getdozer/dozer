# Dozer Ingestion

This module implements several connectors that can act as a source in either real-time or batch fashion. 
Each of the connectors implements a [set of methods defined](https://github.com/getdozer/dozer/blob/main/dozer-ingestion/src/connectors/mod.rs#L32) to support being a source to the data pipeline.

### Available Connectors
| Connector                          |   Status    | Type           |  Schema Mapping   | History Support | Frequency | Implemented Via |
| :--------------------------------- | :---------: | :------------- | :---------------: | :-------------: | :-------- | :-------------- |
| Postgres                           | Available ✅ | Relational     |      Source       |        ✅        | Real Time | Direct          |
| Ethereum                           | Available ✅ | Blockchain     | Logs/Contract ABI |        ✅        | Real Time | Direct          |
| Snowflake                          | Available ✅ | Data Warehouse |      Source       |        ✅        | Polling   | Direct          |
| MySQL                              | Available ✅ | Relational     |      Source       |        ✅        | Real Time | Debezium        |
| Mongodb                            | Available ✅ | NoSQL          |      Source       |        ✅        | Real Time | Debezium        |
| Local Files(CSV, Parquet)          |    Beta     | Object Storage |      Source       |        ✅        | Polling   | Data Fusion     |
| AWS S3(CSV, Parquet)               |    Beta     | Object Storage |      Source       |        ✅        | Polling   | Data Fusion     |
| Google Cloud Storage(CSV, Parquet) |    Beta     | Object Storage |      Source       |        ✅        | Polling   | Data Fusion     |
| Google Sheets                      | In Roadmap  | Applications   |      Source       |        ✅        |           |                 |
| Airtable                           | In Roadmap  | Applications   |      Source       |        ✅        |           |                 |
| Delta Lake                         | In Roadmap  | Data Warehouse |      Source       |        ✅        |           |                 |
| Solana                             | In Roadmap  | Blockchain     | Logs/Contract ABI |        ✅        |           |                 |
| Kafka                              | In Roadmap  | Stream         |  Schema Registry  |        ✅        |           |                 |
| Red Panda                          | In Roadmap  | Stream         |  Schema Registry  |        ✅        |           |                 |
| Pulsar                             | In Roadmap  | Stream         |  Schema Registry  |        ✅        |           |                 |





