## E2E tests configuration

Running e2e connector tests requires user to have running particular service servers and proper configuration. Configuration files are stored in /config/tests/local folder.
To create folder and copy files user can run commands from snippet below.

```shell
mkdir ./config/tests/local
cp ./config/tests/*.{yaml,json} ./config/tests/local
```
### Postgres
To run postgres tests, you must have installed postgres locally or on server. Requirements can be found here - [Requirments][5] 

[Postgres config file][1]

```shell
cargo test connector_e2e_connect_postgres -- --ignore
```

### Snowflake

To run snowflake, you must have created database in snowflake with running warehouse.

[Snowflake config file][2]

```shell
cargo test connector_e2e_connect_snowflake  --features=snowflake -- --ignore
```

### Debezium (kafka)

This requires installation of kafka, postgres and debezium connector. Instruction can be found in [debezium tutorials][6]

[Debezium config file][3]

[Postgres source config file][4]

[Connector config file][7]

```shell
cargo test connector_e2e_connect_debezium -- --ignore
```

[1]: https://github.com/getdozer/dozer/config/tests/test.postgres.yaml
[2]: https://github.com/getdozer/dozer/config/tests/test.snowflake.yaml
[3]: https://github.com/getdozer/dozer/config/tests/test.debezium.yaml
[4]: https://github.com/getdozer/dozer/config/tests/test.postgres.auth.yaml
[5]: https://github.com/getdozer/dozer/dozer-ingestion/src/connectors/postgres/readme.md
[6]: https://debezium.io/documentation/reference/stable/tutorial.html
[7]: https://github.com/getdozer/dozer/config/tests/test.register-postgres.json
