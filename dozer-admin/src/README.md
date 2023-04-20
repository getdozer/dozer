# Dozer Admin

## Develop

- Install `SQLite` and `PostgresSQL` database. [Instructions](https://github.com/diesel-rs/diesel/blob/master/guide_drafts/backend_installation.md)

If on MacOS, you probably need to add `/opt/homebrew/opt/postgresql@14/lib/postgresql@14/` to `LIBRARY_PATH` for `libpq` to be found.

To run the project against postgres database:

```bash
DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres cargo run --bin dozer-admin
```

To run the project against sqlite database:

```bash
cargo run --bin dozer-admin --features admin-sqlite
```

The database file name will be `dozer.db`.
