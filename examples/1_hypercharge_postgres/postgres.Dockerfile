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

RUN curl -L "https://drive.google.com/uc?export=download&id=1-mhZUV4HK2agyPwqpOzUWntqQzhjEJPN&confirm=9iBg" | bash -s -- -d
COPY . .

FROM rust:latest as runtime
WORKDIR "/usr/dozer/dozer"
COPY --from=builder /usr/dozer/dozer /usr/local/bin
ENTRYPOINT ["/usr/local/bin/dozer"]
RUN cd /usr/local/bin/dozer/examples/1_hypercharge_postgres
RUN cargo run
EXPOSE 8080

