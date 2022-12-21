FROM postgres as builder
WORKDIR "/var/lib/stock-sample"
RUN apt-get update && apt-get install -y wget

COPY . .
RUN echo "Downloading Test Files"
RUN export LANG=en_US.UTF-8
RUN export LC_ALL=$LANG
RUN rm -rf ./data
RUN mkdir -p ./data
RUN wget --quiet --load-cookies /tmp/cookies.txt  \
    "https://docs.google.com/uc?export=download&confirm=$(wget --quiet --save-cookies /tmp/cookies.txt  \
    --keep-session-cookies --no-check-certificate 'https://docs.google.com/uc?export=download&id=$1' -O-  \
    | sed -rn 's/.*confirm=([0-9A-Za-z_]+).*/\1\n/p')&id=1yojQwMtNPIKWMURhkA2Hd9JTQIk2LrLV" -O  \
    "./data/stock_price_data.tar.gz" && rm -rf /tmp/cookies.txt

WORKDIR "/var/lib/stock-sample/data"
RUN tar -xzf stock_price_data.tar.gz
RUN rm stock_price_data.tar.gz
