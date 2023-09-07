FROM mysql:8

COPY ./download.sh .
RUN sh download.sh
RUN mv ./data/init.sql /docker-entrypoint-initdb.d/init.sql

COPY ./is_ready.sh /usr/bin/
