## End-to-end Hypercharge Postgres Example

Features used:
- Init project from yaml
- Ingestion from postgresql source
- SQL execution

### Initialize Postgres with Stock Price Datasets

Check out [kaggle](https://www.kaggle.com/datasets/jacksoncrow/stock-market-dataset) for more details on this open source dataset.

```bash
# Remove headers from all tables
sed -i '' 1d *.csv

# Combine tables into one csv
INDEX=0
for f in *.csv
do
	if [[ ${INDEX} -le 2 ]]; then
		INDEX=${INDEX}+1
		sed -e "s/^/${f%.*},/" $f >> combined_small.csv
	fi
done
```

```sql
SELECT 'CREATE DATABASE stocks'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'stocks');

CREATE SCHEMA IF NOT EXISTS public;

CREATE SEQUENCE IF NOT EXISTS stocks_id_seq;

CREATE TABLE IF NOT EXISTS stocks (
                                      id integer NOT NULL DEFAULT nextval('stocks_id_seq') CONSTRAINT stocks_pk PRIMARY KEY,
                                      Ticker varchar(255) NOT NULL,
                                      Date date NOT NULL,
                                      Open double precision NOT NULL,
                                      High double precision NOT NULL,
                                      Low double precision NOT NULL,
                                      Close double precision NOT NULL,
                                      Adj_Close double precision NOT NULL,
                                      Volume integer NOT NULL
);

COPY stocks (Ticker, Date, Open, High, Low, Close, Adj_Close, Volume) FROM
    '<absolute-path-to-csv>'
    WITH (FORMAT csv);
```

## Run

```bash
docker-compose up --build
```

```bash
cargo run
```
