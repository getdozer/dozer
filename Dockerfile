FROM rust:latest as builder
WORKDIR "/usr/dozer"

RUN curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v23.3/protoc-23.3-linux-x86_64.zip
RUN unzip protoc-23.3-linux-x86_64.zip -d $HOME/.local
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
      pkg-config

ENV PATH="$PATH:/root/.local/bin"
RUN echo $(protoc --version)
COPY . .
RUN cargo build --release --bin dozer --features snowflake


FROM rust:latest as runtime
WORKDIR "/usr/dozer"

RUN apt-get update && apt-get install -y unixodbc-dev unixodbc protobuf-compiler
RUN curl -LO https://sfc-repo.snowflakecomputing.com/odbc/linux/2.25.12/snowflake-odbc-2.25.12.x86_64.deb
RUN dpkg -i snowflake-odbc-2.25.12.x86_64.deb
RUN rm snowflake-odbc-2.25.12.x86_64.deb

COPY --from=builder /usr/dozer/target/release/dozer /usr/local/bin
COPY --from=builder /usr/dozer/dozer-config.yaml /usr/dozer

ENTRYPOINT ["/usr/local/bin/dozer"]
EXPOSE 8080
