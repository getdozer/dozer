FROM ubuntu:20.04

RUN apt-get update \
  && apt-get install -y \
      libssl-dev \
      odbcinst \
      unixodbc \
      curl \
      unzip \
      libaio1

# INSTALL PROTOBUF
RUN curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v3.18.1/protoc-3.18.1-linux-x86_64.zip 
RUN unzip protoc-3.18.1-linux-x86_64.zip -d /usr/local/protoc
RUN rm protoc-3.18.1-linux-x86_64.zip

# INSTALL SNOWFLAKE DRIVERS
RUN curl -LO https://sfc-repo.snowflakecomputing.com/odbc/linux/2.25.7/snowflake-odbc-2.25.7.x86_64.deb
RUN dpkg -i snowflake-odbc-2.25.7.x86_64.deb
RUN rm snowflake-odbc-2.25.7.x86_64.deb

RUN curl -LO https://download.oracle.com/otn_software/linux/instantclient/1922000/instantclient-basic-linux.x64-19.22.0.0.0dbru.zip
RUN unzip instantclient-basic-linux.x64-19.22.0.0.0dbru.zip -d /usr/local/instantclient_19_22
ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/local/instantclient_19_22"
RUN ldconfig

# INSTALL DOZER 
RUN echo "Installing dozer binary"

COPY target/release/dozer /usr/local/bin/

ENV PATH="$PATH:/usr/local/protoc/bin"

WORKDIR /usr/dozer
ENTRYPOINT ["dozer"]
CMD ["run"]
