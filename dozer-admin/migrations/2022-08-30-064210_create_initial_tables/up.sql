-- Your SQL goes here
PRAGMA foreign_keys = ON;
create table apps (
    id TEXT NOT NULL PRIMARY KEY,
    name TEXT not null,
    home_dir TEXT,
    flags TEXT,
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
create table sources (
    id TEXT NOT NULL PRIMARY KEY,
    app_id TEXT not null,
    name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    connection_id TEXT NOT NULL,
    columns TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    FOREIGN KEY(app_id) REFERENCES apps(id),
    FOREIGN KEY(connection_id) REFERENCES connections(id)
);
create table endpoints (
    id TEXT NOT NULL PRIMARY KEY,
    app_id TEXT not null,
    name TEXT NOT NULL,
    path TEXT NOT NULL,
    sql TEXT NOT NULL,
    primary_keys TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    FOREIGN KEY(app_id) REFERENCES apps(id)
);
create table source_endpoints (
    source_id TEXT NOT NULL,
    endpoint_id TEXT NOT NULL,
    app_id TEXT not null,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (source_id, endpoint_id),
    FOREIGN KEY(app_id) REFERENCES apps(id),
    FOREIGN KEY(endpoint_id) REFERENCES endpoints(id),
    FOREIGN KEY(source_id) REFERENCES sources(id)
);
create table configs (
    id TEXT NOT NULL PRIMARY KEY,
    app_id TEXT not null,
    api_security TEXT,
    rest TEXT,
    grpc TEXT,
    auth BOOLEAN,
    api_internal TEXT,
    pipeline_internal TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    FOREIGN KEY(app_id) REFERENCES apps(id)
);