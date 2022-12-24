FROM ubuntu:20.04
WORKDIR "/var/lib/stock-sample"
RUN apt-get update \
  && apt-get install -y \
    wget
COPY ./download_stocks.sh .
RUN ./download_stocks.sh 