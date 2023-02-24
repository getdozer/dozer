# Financial Data Postgres Sample

## Initialize Postgres

```curl
# pwd -> dozer-samples/banking-postgres

./scripts/download_banking.sh
```

## Run Postgres & Dozer

```bash
docker-compose -f docker-compose-pg.yml up
```

```bash
docker run -it \
  -v "$PWD":/usr/dozer \
  -p 8080:8080 \
  -p 50051:50051 \
  --platform linux/amd64 \
  public.ecr.aws/getdozer/dozer:dev \
  dozer
```
### Source schema

```sql
--
-- Name: completedacct; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE completedacct (
   account_id text PRIMARY KEY,
   district_id integer,
   frequency text,
   parseddate date,
   year integer,
   month integer,
   day integer
);

--
-- Name: completedcard; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE completedcard (
   card_id text PRIMARY KEY,
   disp_id text,
   type text,
   year integer,
   month integer,
   day integer,
   fulldate date
);
--
-- Name: completedclient; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE completedclient (
     client_id text PRIMARY KEY,
     sex text,
     fulldate date,
     day integer,
     month integer,
     year integer,
     age integer,
     social text,
     first text,
     middle text,
     last text,
     phone text,
     email text,
     address_1 text,
     address_2 text,
     city text,
     state text,
     zipcode integer,
     district_id integer
);
--
-- Name: completeddisposition; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE completeddisposition (
  disp_id text PRIMARY KEY,
  client_id text,
  account_id text,
  type text
);

--
-- Name: completeddistrict; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE completeddistrict (
    district_id integer PRIMARY KEY,
    city text,
    state_name text,
    state_abbrev text,
    region text,
    division text
);

--
-- Name: completedloan; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE completedloan (
    loan_id text PRIMARY KEY,
    account_id text,
    amount integer,
    duration integer,
    payments integer,
    status text,
    year integer,
    month integer,
    day integer,
    fulldate date,
    location integer,
    purpose text
);

--
-- Name: completedorder; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE completedorder (
    order_id integer PRIMARY KEY,
    account_id text,
    bank_to text,
    account_to integer,
    amount numeric,
    k_symbol text
);

--
-- Name: completedtrans; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE completedtrans (
    id text,
    trans_id text PRIMARY KEY,
    account_id text,
    type text,
    operation text,
    amount numeric,
    balance numeric,
    k_symbol text,
    bank text,
    account text,
    year integer,
    month integer,
    day integer,
    fulldate date,
    fulltime text,
    fulldatewithtime text
);
```

## Expected Output

```bash

____   ___ __________ ____
|  _ \ / _ \__  / ____|  _ \
| | | | | | |/ /|  _| | |_) |
| |_| | |_| / /_| |___|  _ <
|____/ \___/____|_____|_| \_\


Dozer Version: 0.1.7

INFO Initiating app: postgres-advance-banking    
INFO Home dir: ./.dozer    
INFO [API] Configuration    
+------+---------+-------+
| Type | IP      | Port  |
+------+---------+-------+
| REST | 0.0.0.0 | 8080  |
+------+---------+-------+
| GRPC | 0.0.0.0 | 50051 |
+------+---------+-------+
INFO [API] Endpoints    
+---------------+--------------+-----+
| Path          | Name         | Sql |
+---------------+--------------+-----+
| /dist_clients | dist_clients |     |
+---------------+--------------+-----+
INFO [banking] Connection parameters    
+----------+----------------------+
| user     | postgres             |
+----------+----------------------+
| password | *************        |
+----------+----------------------+
| host     | host.docker.internal |
+----------+----------------------+
| port     | 5433                 |
+----------+----------------------+
| database | banking              |
+----------+----------------------+
INFO [banking] ✓ Connection validation completed    
INFO [banking][completeddistrict] ✓ Schema validation completed    
INFO [banking][completedclient] ✓ Schema validation completed   
```
