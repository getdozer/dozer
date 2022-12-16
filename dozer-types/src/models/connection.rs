use crate::ingestion_types::EthFilter;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Default, Hash)]
pub struct Connection {
    pub db_type: DBType,
    pub authentication: Authentication,
    pub name: String,
    pub id: Option<String>,
}
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Default, Hash)]
pub enum DBType {
    Postgres,
    Ethereum,
    #[default]
    Events,
    Snowflake,
    Kafka,
}
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash)]
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
        driver: Option<String>,
    },
    KafkaAuthentication {
        broker: String,
        topic: String,
        schema_registry_url: Option<String>,
    },
}
impl Default for Authentication {
    fn default() -> Self {
        Authentication::Events {}
    }
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
            "Kafka" | "kafka" => Ok(DBType::Kafka),
            _ => Err("Not match any value in Enum DBType"),
        }
    }
}
