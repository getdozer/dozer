FROM rust:latest as builder
WORKDIR "/usr/dozer"
RUN apt-get update && apt-get install -y \
      build-essential \
      make \
      g++ \
      libclang-dev \
      protobuf-compiler \
      devscripts \
      debhelper \
      build-essential \
      libssl-dev \
      pkg-config

COPY . .
RUN cargo build --release --bin dozer



FROM rust:latest as runtime
WORKDIR "/usr/dozer"
COPY --from=builder /usr/dozer/target/release/dozer /usr/local/bin
COPY --from=builder /usr/dozer/config/log4rs.release.yaml /usr/local/bin
COPY --from=builder /usr/dozer/tests/simple_e2e_example/dozer-config.yaml /usr/local/
ENTRYPOINT ["/usr/local/bin/dozer"]
EXPOSE 8080