FROM debezium/postgres:15
WORKDIR "/var/lib/stock-sample"
RUN 
RUN apt-get update \
  && apt-get install -y \
    wget
COPY ./download_stocks.sh .
RUN ./download_stocks.sh

RUN localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8
