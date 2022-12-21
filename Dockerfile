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
#RUN curl -L "https://drive.google.com/uc?export=download&id=1-mhZUV4HK2agyPwqpOzUWntqQzhjEJPN&confirm=9iBg" | bash
RUN cargo build --release

FROM rust:latest as runtime
WORKDIR "/usr/dozer"
#COPY --from=builder /usr/dozer /usr/local/bin
COPY --from=builder /usr/dozer/target/release/dozer /usr/local/bin
ENTRYPOINT ["/usr/local/bin/dozer"]
EXPOSE 8080