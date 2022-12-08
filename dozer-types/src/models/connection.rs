use crate::ingestion_types::EthFilter;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Default)]
pub struct Connection {
    pub db_type: DBType,
    pub authentication: Authentication,
    pub name: String,
    pub id: Option<String>,
}
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Enumeration)]
#[repr(i32)]
pub enum DBType {
    Postgres = 0,
    Snowflake = 1,
    Ethereum = 2,
    Events = 3,
    Kafka = 4,
}
impl DBType {
    pub fn as_str_name(&self) -> &'static str {
        match self {
            DBType::Postgres => "postgres",
            DBType::Snowflake => "snowflake",
            DBType::Ethereum => "ethereum",
            DBType::Events => "events",
            DBType::Kafka => "kafka",
        }
    }
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
        driver: Option<String>,
    },
    KafkaAuthentication {
        broker: String,
        topic: String,
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
            "Events" | "events" => Ok(DBType::Events),
            _ => Err("Not match any value in Enum DBType"),
        }
    }
}
