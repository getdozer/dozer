-- Your SQL goes here
create table apps (
    id TEXT NOT NULL PRIMARY KEY,
    name TEXT not null,
    config TEXT not null,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);
create table connections (
    id TEXT NOT NULL PRIMARY KEY,
    config TEXT NOT NULL,
    name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);