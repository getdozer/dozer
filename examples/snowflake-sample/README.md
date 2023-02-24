## Snowflake example
Before running this example you need to have snowflake accounts credentials and set if for env
```
export SN_SERVER={snowflake server}
export SN_USER={username}
export SN_PASSWORD={password}
export SN_DATABASE={database}
export SN_WAREHOUSE={warehouse}
```

Before starting container, you need to create tables with data in snowflake
```sql
CREATE TABLE CUSTOMERS LIKE SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER;
CREATE TABLE ORDERS LIKE SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS;

ALTER TABLE PUBLIC.CUSTOMERS
    ADD CONSTRAINT customers_PK
        PRIMARY KEY (C_CUSTKEY);

ALTER TABLE PUBLIC.ORDERS
    ADD CONSTRAINT orders_PK
        PRIMARY KEY (O_ORDERKEY);

INSERT INTO CUSTOMERS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER LIMIT 100000;
INSERT INTO ORDERS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS LIMIT 1000000;
```

Then you can run docker container
```bash
docker-compose up
```

