-- Your SQL goes here
create table connections
(
    id TEXT NOT NULL PRIMARY KEY,
    auth TEXT default '{}' not null,
    db_type text not null
);

create table sources
(
    id   TEXT NOT NULL PRIMARY KEY,
    table_name            text,
    connection_id         TEXT
        constraint foreign_key_name
            references connections (id),
    connection_table_name text,
    data_layout           text default '{}',
    refresh_options       text default '{}'
);