-- Your SQL goes here
PRAGMA foreign_keys = ON;
create table apps (
    id TEXT NOT NULL PRIMARY KEY,
    name TEXT not null,
    config TEXT not null,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);
create table connections (
    id TEXT NOT NULL PRIMARY KEY,
    app_id TEXT NOT NULL,
    auth TEXT NOT NULL,
    name TEXT NOT NULL,
    db_type text NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    FOREIGN KEY(app_id) REFERENCES apps(id)
);