FROM debezium/postgres:13

ADD ./pg_hba.conf /var/lib/banking/pg_hba.conf
Add ./scripts/banking_data.sql /docker-entrypoint-initdb.d/init.sql

CMD postgres -c hba_file=/var/lib/banking/pg_hba.conf