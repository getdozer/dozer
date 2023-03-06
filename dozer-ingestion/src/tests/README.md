### Connectors tests

As connectors connects to external databases, we need to have running databases instances. 
To do that, we will create docker containers using this command.

```bash
docker-compose -f dozer-ingestion/src/tests/connections/postgres/docker-compose.yaml up
```

After we have running containers, tests can be executed with this command
```bash
cargo test test_connector_ -- --ignored
```

### Snowflake tests

As snowflake is cloud based database, we cannot create container for it, so to execute tests we need to have credentials.
Credentials can be set with such command

```bash
export SN_SERVER={sn_server}
export SN_USER={sn_user}
export SN_PASSWORD={sn_password}
export SN_DATABASE={sn_database}
export SN_WAREHOUSE={sn_warehouse}
```

After settings is properly set, tests can be executed with such command

```cargo test snowflake -- --ignored```