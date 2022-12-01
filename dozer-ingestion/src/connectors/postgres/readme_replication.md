# Replica identity and mapped operation

```sql
CREATE SEQUENCE users_id_seq;

CREATE TABLE users
(
    id INTEGER NOT NULL DEFAULT nextval('users_id_seq')
        CONSTRAINT users_pk
            PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    phone VARCHAR(255) NOT NULL 
);

CREATE UNIQUE INDEX email_index
    ON users (email);

ALTER SEQUENCE users_id_seq
    OWNED BY users.id;

```
Main difference between replica identity types is what old record data is sent with `update` and `delete` operations

## DEFAULT
By default on update and delete events only sending primary key from old record

```sql
-- Change of replication identity
ALTER TABLE users REPLICA IDENTITY DEFAULT;
```

```sql
-- Example of update
BEGIN;
INSERT INTO users (email, phone) VALUES ('test1@email.com', '98765421');
COMMIT;

BEGIN;
UPDATE users SET phone = '99339439439' WHERE email = 'test1@email.com';
COMMIT;
```


#### Example of update replication messages in update transaction

| Replication message                                                                          | Operation                                                                                                                                                                                                                                                                               |
|----------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `BEGIN (transaction id)`                                                                     |                                                                                                                                                                                                                                                                                         |
| ```UPDATE (old: { id: 1}, new: {id: 1, phone: '99339439439', 'email': 'test1@email.com'})``` | <pre>OperationEvent(<br>  Operation::Update {<br>    new: Record {schema_id: 1,values: vec![Field::Int(1), Field::String('test1@email.com'), Field::String('99339439439')],},<br>    old: Record {schema_id: 1,values: vec![Field::Int(1), Field::Null, Field::Null],<br>  }<br>}</pre> |
| `COMMIT (commit_lsn)`                                                                        |                                                                                                                                                                                                                                                                                         |

## USING INDEX
When using index as replica identity only values used in unique key are sent

```sql
-- Change of replication identity
ALTER TABLE users REPLICA IDENTITY USING INDEX email_index;
```

```sql
-- Example of update
BEGIN;
INSERT INTO users (email, phone) VALUES ('test2@email.com', '98765422');
COMMIT;

BEGIN;
UPDATE users SET phone = '99339439440' WHERE email = 'test2@email.com';
COMMIT;
```

#### Example of update replication messages in update transaction

| Replication message                                                                                             | Operation                                                                                                                                                                                                                                                                                                  |
|-----------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `BEGIN (transaction id)`                                                                                        |                                                                                                                                                                                                                                                                                                            |
| ```UPDATE (old: { email: 'test2@email.com'}, new: {id: 2, phone: '99339439440', 'email': 'test2@email.com'})``` | <pre>OperationEvent(<br>  Operation::Update {<br>    new: Record {schema_id: 1,values: vec![Field::Int(2), Field::String('test2@email.com'), Field::String('99339439440')],},<br>    old: Record {schema_id: 1,values: vec![Field::Null, Field::String("test2@email.com"), Field::Null],<br>  }<br>}</pre> |
| `COMMIT (commit_lsn)`                                                                                           |                                                                                                                                                                                                                                                                                                            |


## FULL
When using full mode as replica identity, all row values are sent during update
```sql
-- Change of replication identity
ALTER TABLE users REPLICA IDENTITY FULL;
```

```sql
-- Example of update
BEGIN;
INSERT INTO users (email, phone) VALUES ('test3@email.com', '98765423');
COMMIT;

BEGIN;
UPDATE users SET phone = '99339439441' WHERE email = 'test3@email.com';
COMMIT;
```

#### Example of update replication messages in update transaction

| Replication message                                                                                                                       | Operation                                                                                                                                                                                                                                                                                                                  |
|-------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `BEGIN (transaction id)`                                                                                                                  |                                                                                                                                                                                                                                                                                                                            |
| ```UPDATE (old: { email: 'test3@email.com', phone: '98765423', id: 3}, new: {id: 3, phone: '99339439441', 'email': 'test3@email.com'})``` | <pre>OperationEvent(<br>  Operation::Update {<br>    new: Record {schema_id: 1,values: vec![Field::Int(3), Field::String('test3@email.com'), Field::String('99339439441')],},<br>    old: Record {schema_id: 1,values: vec![Field::Int(3), Field::String("test3@email.com"), Field::String("98765423")],<br>  }<br>}</pre> |
| `COMMIT (commit_lsn)`                                                                                                                     |                                                                                                                                                                                                                                                                                                                            |



## NOTHING
Nothing is sent from old row when using this type

```sql
-- Change of replication identity
ALTER TABLE users REPLICA IDENTITY NOTHING;
```

```sql
-- Example of update
BEGIN;
INSERT INTO users (email, phone) VALUES ('test4@email.com', '98765424');
COMMIT;

BEGIN;
UPDATE users SET phone = '99339439442' WHERE email = 'test4@email.com';
COMMIT;
```

#### Example of update replication messages in update transaction

| Replication message                                                           | Operation                                                                                                                                                                                                                                                                             |
|-------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `BEGIN (transaction id)`                                                      |                                                                                                                                                                                                                                                                                       |
| ```UPDATE (new: {id: 4, phone: '99339439442', 'email': 'test4@email.com'})``` | <pre>OperationEvent(<br>  Operation::Update {<br>    new: Record {schema_id: 1,values: vec![Field::Int(4), Field::String('test4@email.com'), Field::String('99339439442')],},<br>    old: Record {schema_id: 1,values: vec![Field::Null, Field::Null, Field::Null],<br>  }<br>}</pre> |
| `COMMIT (commit_lsn)`                                                         |                                                                                                                                                                                                                                                                                       |
