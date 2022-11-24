FROM rust:latest as builder
WORKDIR "/usr/dozer"
RUN curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v21.9/protoc-21.9-linux-aarch_64.zip
RUN unzip protoc-21.9-linux-aarch_64.zip -d $HOME/.local
ENV PATH="$PATH:$HOME/.local/bin"
RUN apt-get update && apt-get install -y \
      build-essential \
      make \
      g++ \
      libclang-dev \
      devscripts \
      debhelper \
      build-essential \
      libssl-dev \
      pkg-config \
      unixodbc-dev

ENV PATH="$PATH:/root/.local/bin"
RUN protoc --version
COPY . .
RUN cargo build --release --bin dozer --features snowflake



FROM rust:latest as runtime
WORKDIR "/usr/dozer"
RUN apt-get update && apt-get install -y unixodbc-dev unixodbc
RUN curl -LO https://sfc-repo.snowflakecomputing.com/odbc/linuxaarch64/2.25.6/snowflake-odbc-2.25.6.aarch64.deb
RUN dpkg -i snowflake-odbc-2.25.6.aarch64.deb
COPY --from=builder /usr/dozer/target/release/dozer /usr/local/bin
COPY --from=builder /usr/dozer/config/log4rs.release.yaml /usr/dozer
RUN cp /usr/dozer/log4rs.release.yaml /usr/local/bin/log4rs.yaml
RUN cp /usr/dozer/log4rs.release.yaml /usr/dozer/log4rs.yaml
COPY --from=builder /usr/dozer/tests/connectors/snowflake/dozer-config.yaml /usr/dozer
ENTRYPOINT ["/usr/local/bin/dozer"]
EXPOSE 8080