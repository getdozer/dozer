# Dozer Tests

This crate contains the end-to-end test framework and the test client used in the framework (and some unit tests, which are not covered in this README).

## Run Tests Locally

To run the e2e tests locally, first build the debug dozer binary:

```rust
cargo build --bin dozer
```

The test framework expects the binary located at `target/debug/dozer`. You can override that by setting `DOZER_BIN` environment variable.

Then run the `dozer-tests` binary:

```rust
cargo run --bin dozer-tests
```

It will run all the test cases and stop on first failure.

Filtering of test cases similar to `cargo test` is supported. For example, to run all test cases whose names start with `eth`:

```rust
cargo run --bin dozer-tests -- eth
```

The `--` before `eth` is how cargo knows following arguments are for the binary.

### Expected Environment Variables for Running the Tests

- ETH_WSS_URL for Ethereum connector.

## Add a Test Case

All test cases are located under `dozer-tests/src/e2e_tests/cases`. Each case input is within a separate directory, the directory name being the test case name.

Each test case should have a `dozer-config.yaml` file, which `dozer` runs with.

If `expectations.json` is found in the case directory, the framework expects `dozers` to start successfully with the given config file, and it will check if all the expectations are met.

For all supported expectation checks, see `expectations.rs`.

Otherwise, if `error.json` is found, the framework expects `dozer` fails to start with the given config file, and it will check if `dozer` fails with the expected error.

For all supported error expectation checks, see `expectations.rs`.

## Run Tests Like in CI

```rust
cargo build --bin dozer-test-client
cargo run --bin dozer-tests -- -r buildkite
```

If `buildkite` runner type is used, the framework will not use local `dozer` binary or run test client in-process. Instead, it creates `docker-compose.yaml` files for the test cases, which contains all the sources, the `dozer` service and the test client. A separate `docker compose` process is started to validate all the expectations using the client.

This mode requires you to build `dozer-test-client` binary first, and the artifact must be located at `target/debug/dozer-test-client`.
