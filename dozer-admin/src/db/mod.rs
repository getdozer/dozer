#[cfg(feature = "admin-sqlite")]
pub type DB = diesel::sqlite::Sqlite;
#[cfg(feature = "admin-sqlite")]
pub type Connection = diesel::sqlite::SqliteConnection;

#[cfg(not(feature = "admin-sqlite"))]
pub type DB = diesel::pg::Pg;
#[cfg(not(feature = "admin-sqlite"))]
pub type Connection = diesel::pg::PgConnection;

pub mod app;
pub mod connection;
pub mod pool;
pub mod schema;
