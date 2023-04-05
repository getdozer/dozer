set -e

# Check if dozer version matches `DOZER_VERSION`
dozer -V | grep "$DOZER_VERSION"

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# Run grpc ingest test because it doesn't need docker or ETH secrets.
CARGO_TARGET_DIR=../ DOZER_BIN=dozer RUST_LOG=info "$HOME/.cargo/bin/cargo" run -p dozer-tests --bin dozer-tests -- grpc_ingest
