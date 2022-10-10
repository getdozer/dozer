-- Your SQL goes here
create table connections
(
    id TEXT NOT NULL PRIMARY KEY,
    auth TEXT not null,
    name TEXT not null,
    db_type text not null,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL

);

create table sources
(
    id   TEXT NOT NULL PRIMARY KEY,
    name TEXT NOT NULL,
    dest_table_name  TEXT NOT NULL,
    source_table_name TEXT NOT NULL,
    connection_id     TEXT NOT NULL
        constraint foreign_key_name
            references connections (id),
    history_type TEXT NOT NULL,
    refresh_config TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

create table endpoints
(
    id   TEXT NOT NULL PRIMARY KEY,
    name TEXT NOT NULL,
    path  TEXT NOT NULL,
    enable_rest BOOLEAN NOT NULL,
    enable_grpc BOOLEAN NOT NULL,
    sql TEXT NOT NULL,
    data_maper TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

create table source_endpoints (
    source_id TEXT NOT NULL
        constraint foreign_key_name
            references sources (id),
    endpoint_id TEXT NOT NULL
        constraint foreign_key_name
            references endpoints (id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
      PRIMARY KEY (source_id, endpoint_id)

);
