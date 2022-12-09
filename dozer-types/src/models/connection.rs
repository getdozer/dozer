use crate::ingestion_types::{EthConfig, KafkaConfig, SnowflakeConfig};
use serde::{Deserialize, Serialize};
use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    str::FromStr,
};
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message)]
pub struct Connection {
    #[prost(oneof = "Authentication", tags = "1,2,3,4,5")]
    pub authentication: Option<Authentication>,
    #[prost(string, optional, tag = "6")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[prost(string, optional, tag = "7")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_id: Option<String>,
    #[prost(enumeration = "DBType", tag = "8")]
    pub db_type: i32,
    #[prost(string, tag = "9")]
    pub name: String,
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
impl TryFrom<i32> for DBType {
    type Error = Box<dyn Error>;
    fn try_from(item: i32) -> Result<Self, Self::Error> {
        match item {
            0 => Ok(DBType::Postgres),
            1 => Ok(DBType::Snowflake),
            2 => Ok(DBType::Ethereum),
            3 => Ok(DBType::Events),
            4 => Ok(DBType::Kafka),
            _ => Err("DBType enum not match".to_owned())?,
        }
    }
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
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message)]
pub struct PostgresAuthentication {
    #[prost(string, tag = "1")]
    pub user: String,
    #[prost(string, tag = "2")]
    pub password: String,
    #[prost(string, tag = "3")]
    pub host: String,
    #[prost(uint32, tag = "4")]
    pub port: u32,
    #[prost(string, tag = "5")]
    pub database: String,
}
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message)]
pub struct EventsAuthentication {}
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Oneof)]
pub enum Authentication {
    #[prost(message, tag = "1")]
    Postgres(PostgresAuthentication),
    #[prost(message, tag = "2")]
    Ethereum(EthConfig),
    #[prost(message, tag = "3")]
    Events(EventsAuthentication),
    #[prost(message, tag = "4")]
    Snowflake(SnowflakeConfig),
    #[prost(message, tag = "5")]
    Kafka(KafkaConfig),
}

impl Default for Authentication {
    fn default() -> Self {
        Authentication::Postgres(PostgresAuthentication::default())
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

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, ::prost::Message)]
pub struct AuthenticationWrapper {
    #[prost(oneof = "Authentication", tags = "1,2,3,4,5")]
    pub authentication: Option<Authentication>,
}

impl From<AuthenticationWrapper> for Authentication {
    fn from(input: AuthenticationWrapper) -> Self {
        return input.authentication.unwrap_or_default();
    }
}
