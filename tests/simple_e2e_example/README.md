## End to end sample

Features used:
- Init project from yaml
- Ingestion from postgresql source
- SQL execution

### Initialize Postgres with users table
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

## Run

```bash
docker-compose up --build
```
