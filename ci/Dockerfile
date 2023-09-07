FROM ubuntu:20.04

RUN apt-get update \
  && apt-get install -y \
      libssl-dev \
      odbcinst \
      unixodbc \
      curl \
      unzip

# INSTALL PROTOBUF
RUN curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v3.18.1/protoc-3.18.1-linux-x86_64.zip
RUN unzip protoc-3.18.1-linux-x86_64.zip -d /usr/local/protoc
RUN rm protoc-3.18.1-linux-x86_64.zip

# INSTALL SNOWFLAKE DRIVERS
RUN curl -LO https://sfc-repo.snowflakecomputing.com/odbc/linux/2.25.7/snowflake-odbc-2.25.7.x86_64.deb
RUN dpkg -i snowflake-odbc-2.25.7.x86_64.deb
RUN rm snowflake-odbc-2.25.7.x86_64.deb

# INSTALL ONNX RUNTIME
RUN curl -LO https://github.com/microsoft/onnxruntime/releases/download/v1.15.1/onnxruntime-linux-x64-1.15.1.tgz
RUN tar -xf onnxruntime-linux-x64-1.15.1.tgz -C /usr/local
RUN rm onnxruntime-linux-x64-1.15.1.tgz
RUN cp /usr/local/onnxruntime-linux-x64-1.15.1/lib/libonnxruntime.so.1.15.1 /usr/local/lib/

# INSTALL DOZER 
RUN echo "Installing dozer binary"

COPY target/release/dozer /usr/local/bin/

ENV PATH="$PATH:/usr/local/protoc/bin"

WORKDIR /usr/dozer
ENTRYPOINT ["dozer"]
CMD ["run"]
