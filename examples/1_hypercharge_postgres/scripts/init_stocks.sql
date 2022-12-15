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
    '/var/lib/stock-sample/data/stock_price.csv'
    WITH (FORMAT csv);