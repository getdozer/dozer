FROM debezium/postgres:13

COPY ./pg_hba.conf /var/lib/foo/pg_hba.conf

RUN apt update && apt install -y curl unzip locales-all

COPY ./download.sh .
RUN sh download.sh
RUN mv ./data/init.sql /docker-entrypoint-initdb.d/init.sql

RUN rm -rf /var/lib/postgresql/data
