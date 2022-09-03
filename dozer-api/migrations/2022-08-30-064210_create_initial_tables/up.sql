-- Your SQL goes here
create table connections
(
    id   integer not null
        constraint pk
            primary key autoincrement,
    auth TEXT default '{}' not null,
    type text
);

create table sources
(
    id                    integer not null
        primary key autoincrement,
    table_name            text,
    connection_id         integer
        constraint foreign_key_name
            references connections (id),
    connection_table_name text,
    data_layout           text default '{}',
    refresh_options       text default '{}'
);