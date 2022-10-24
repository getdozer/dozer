
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

cd scripts/tests/src/films
docker-compose up --build
sh ./scripts/download_and_insert.sh
docker exec -d dozer-orchestrator ./target/debug/dozer-schema
docker exec dozer-orchestrator ./target/debug/dozer-orchestrator run -c ./tests/src/films/dozer-run.yaml
sh ./scripts/run_test.sh
