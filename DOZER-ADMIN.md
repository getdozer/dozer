## PREINSTALL

- [Yarn](https://yarnpkg.com/getting-started/install)
- [Serve](https://yarnpkg.com/package/)
- [Cargo Make](https://github.com/sagiegurari/cargo-make#installation)
- [Gh CLi](https://cli.github.com/)
- [Diesel](https://github.com/diesel-rs/diesel/tree/master/diesel_cli)

  ```
  cargo install diesel_cli --no-default-features --features "sqlite"
  ```

## Run local debug with source

```
  cargo make --no-workspace admin-local
```

Open [http://localhost:3000](http://localhost:3000/)

## Run Release binary

```
  cargo make --no-workspace admin-release
```

this one will generate 4 parts in `target/release` folder

- dozer
- dozer-admin
- dozer-admin-config.yaml
- ui

To open `dozer-admin` simply run

```
cd target/release && ./dozer-admin
```
Open [http://localhost:3000](http://localhost:3000/)