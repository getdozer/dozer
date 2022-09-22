use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Connection {
    pub db_type: DBType,
    pub authentication: Authentication,
    pub name: String,
    pub id: Option<String>,
}
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum DBType {
    Postgres,
    Databricks,
    Snowflake,
}
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum Authentication {
    PostgresAuthentication {
        user: String,
        password: String,
        host: String,
        port: u32,
        database: String,
    },
}
