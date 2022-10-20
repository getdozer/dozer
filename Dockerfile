FROM rust:latest

WORKDIR "/usr/dozer"

RUN apt-get update
RUN apt-get install -y curl bash unzip build-essential autoconf automake libtool make g++ libclang-dev
RUN apt-get install --no-install-recommends --assume-yes \
      protobuf-compiler
RUN apt-get -y install devscripts debhelper build-essential fakeroot zlib1g-dev libbz2-dev libsnappy-dev libgflags-dev libzstd-dev
COPY . .

RUN cargo build --bin dozer-orchestrator
RUN cargo build --bin dozer-schema

EXPOSE 8080

VOLUME ["/usr/local/cargo"]