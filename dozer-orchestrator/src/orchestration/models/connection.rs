use std::{fmt::{Display, Formatter, self}, str::FromStr};
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
impl Display for DBType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl FromStr for DBType {
    type Err = &'static str;
    fn from_str(s: &str) -> Result<DBType, Self::Err> {
        println!("DBType from_str input {:?}", s);
        match s {
            "Postgres" | "postgres" => Ok(DBType::Postgres),
            "Databricks" | "databricks" => Ok(DBType::Databricks),
            "Snowflake" | "snowflake" => Ok(DBType::Snowflake),
            _ => Err("Not match any value in Enum DBType"),
        }
    }
}
