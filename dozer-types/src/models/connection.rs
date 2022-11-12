use serde::{Deserialize, Serialize};
use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};

use crate::ingestion_types::EthFilter;
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct Connection {
    pub db_type: DBType,
    pub authentication: Authentication,
    pub name: String,
    pub id: Option<String>,
}
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum DBType {
    Postgres,
    Ethereum,
    Events,
    Snowflake,
}
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum Authentication {
    PostgresAuthentication {
        user: String,
        password: String,
        host: String,
        port: u16,
        database: String,
    },
    EthereumAuthentication {
        filter: EthFilter,
        wss_url: String,
    },
    Events {},
    SnowflakeAuthentication {
        server: String,
        port: String,
        user: String,
        password: String,
        database: String,
        schema: String,
        warehouse: String,
        driver: String,
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
        match s {
            "Postgres" | "postgres" => Ok(DBType::Postgres),
            "Ethereum" | "ethereum" => Ok(DBType::Ethereum),
            "Snowflake" | "snowflake" => Ok(DBType::Snowflake),
            _ => Err("Not match any value in Enum DBType"),
        }
    }
}
