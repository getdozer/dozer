FROM arm64v8/ubuntu:20.04

RUN apt-get update \
  && apt-get install -y \
      libssl-dev \
      odbcinst \
      unixodbc \
      curl \
      unzip

# INSTALL PROTOBUF
RUN curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v3.18.1/protoc-3.18.1-linux-aarch_64.zip
RUN unzip protoc-3.18.1-linux-aarch_64.zip -d /usr/local/protoc
RUN rm protoc-3.18.1-linux-aarch_64.zip

# INSTALL SNOWFLAKE DRIVERS
RUN curl -LO https://sfc-repo.snowflakecomputing.com/odbc/linuxaarch64/2.25.7/snowflake-odbc-2.25.7.aarch64.deb
RUN dpkg -i snowflake-odbc-2.25.7.aarch64.deb
RUN rm snowflake-odbc-2.25.7.aarch64.deb

# INSTALL DOZER 
RUN echo "Installing dozer binary"

COPY target/release/dozer /usr/local/bin/

ENV PATH="$PATH:/usr/local/protoc/bin"

WORKDIR /usr/dozer
ENTRYPOINT ["dozer"]
CMD ["run"]
