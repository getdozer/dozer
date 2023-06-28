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

The cases starting with `ignore-` are ignored by default. To run them, use `--ignored` flag:

```rust
cargo run --bin dozer-tests -- --ignored
```

Filtering of test cases similar to `cargo test` is supported. For example, to run all test cases whose names start with `eth`:

```rust
cargo run --bin dozer-tests -- eth
```

The `--` before `eth` is how cargo knows following arguments are for the binary.

### Expected Environment Variables for Running the Tests

- ETH_WSS_URL for Ethereum connector based on log.
- ETH_HTTPS_URL for Ethereum connector based on trace.

## Add a Test Case

All test cases are located under `dozer-tests/src/e2e_tests/cases`. Each case input is within a separate directory, the directory name being the test case name.

Each test case should have a `dozer-config.yaml` file, which `dozer` runs with.

If `expectations.json` is found in the case directory, the framework expects `dozers` to start successfully with the given config file, and it will check if all the expectations are met.

For all supported expectation checks, see `expectations.rs`.

Otherwise, if `error.json` is found, the framework expects `dozer` fails to start with the given config file, and it will check if `dozer` fails with the expected error.

For all supported error expectation checks, see `expectations.rs`.

## Add a Connection

Test cases may need to establish various kinds of connections. Connection services can run as a docker compose service.

The framework traverses `dozer-config.yaml`, and for every `connection`, it tries to find a directory with the connection name under `dozer-tests/src/e2e_tests/connections`. If found, it adds the connection service defined in that directory to a docker compose file, and starts the containers before running the test client.

The connection directory may have a `Dockerfile` used for building the image. The build context will be the connection directory.

If there's no `Dockerfile` under the connection directory, it must contain a `service.yaml` file, whose content will be added to the connection service section of the docker compose file. The `build` section of `service.yaml` will be overwritten, to the `Dockerfile` if it exists, removed if not. The working directory of the `docker compose` run will be the repository root, so be careful with relative paths in `service.yaml` (better don't use them).

### Health check the connection

We support all 3 kinds of health checks that docker compose supports.

If a file named`oneshot` is found under the connection directory, health check criteria will be `service_completed_successfully`.

Otherwise, if `service.yaml` contains a `healthcheck` section, health check criteria will be `service_healthy`.

Otherwise, health check criteria will be `service_started`.

### Troubleshoot Connections

- If `service.yaml` parsing fails or generated `docker-compose.yaml` misses some content from it, it's probably because our data model doesn't agree with the file content. You can check the data model at `docker_compose.rs`.

## Run Tests Like in CI

```bash
export DOZER_VERSION=YOUR_TEST_TARGET_VERSION
```

The CI tests use the dozer image instead of a locally built binary. It reads `DOZER_VERSION` environment variable to determine the image tag.

```bash
docker compose -f ./.buildkite/build_dozer_tests/docker-compose.yaml up
```

This command will build `dozer-tests` image, inside which `dozer-tests` and `dozer-test-client` can run. It also builds `dozer-test-client` binary under `target/debug/`, which will be used by test cases. After building is finished, the container runs `dozer-tests` to test all the test cases.

After `dozer-tests` image and `dozer-test-client` binary is built, if you want to run a single test case:

```bash
cargo run --bin dozer-tests -- -r buildkite TEST_CASE_NAME
```

Note that you can't build `dozer-test-client` locally because your local machine's architecture may differ from `dozer-tests` image.
