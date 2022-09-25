-- Your SQL goes here
create table connections
(
    id TEXT NOT NULL PRIMARY KEY,
    auth TEXT default '{}' not null,
    name TEXT not null,
    db_type text not null
);

create table sources
(
    id   TEXT NOT NULL PRIMARY KEY,
    name TEXT NOT NULL,
    dest_table_name  TEXT NOT NULL,
    source_table_name TEXT NOT NULL,
    connection_id         TEXT
        constraint foreign_key_name
            references connections (id),
    history_type TEXT NOT NULL,
    refresh_config TEXT NOT NULL
);

create table endpoints
(
    id   TEXT NOT NULL PRIMARY KEY,
    name TEXT NOT NULL,
    path  TEXT NOT NULL,
    enable_rest BOOLEAN,
    enable_grpc BOOLEAN,
    sql TEXT NOT NULL,
    data_maper TEXT NOT NULL,
    source_ids TEXT NOT NULL,
    history_type TEXT NOT NULL,
    refresh_config TEXT NOT NULL
);
