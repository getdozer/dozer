# Dozer Log JS

NodeJS bindings for Dozer Log

### Pre Requisites
- [cargo-cp-artifact](https://www.npmjs.com/package/cargo-cp-artifact)

### Building
```bash
npm install -g cargo-cp-artifact

# From home directory
cargo-cp-artifact -a cdylib dozer-log-js index.node -- cargo build --message-format=json-render-diagnostics
```

