FROM rust:latest as builder
WORKDIR "/usr/dozer"
RUN apt-get update && apt-get install -y \
      build-essential \
      autoconf \
      automake \
      libtool \
      make \
      g++ \
      libclang-dev \
      protobuf-compiler \
      devscripts \
      debhelper \
      build-essential \
      fakeroot \
      zlib1g-dev \
      libbz2-dev \
      libsnappy-dev \
      libgflags-dev \
      libzstd-dev

COPY . .
RUN cargo build --release --bin dozer



FROM rust:latest as runtime
WORKDIR "/usr/dozer"
COPY --from=builder /usr/dozer/target/release/dozer /usr/local/bin
COPY --from=builder /usr/dozer/log4rs.yaml /usr/local/bin
COPY --from=builder /usr/dozer/tests/simple_e2e_example/dozer-config.yaml /usr/local/
RUN ls
#ENTRYPOINT ["/usr/local/bin/dozer"]
EXPOSE 8080