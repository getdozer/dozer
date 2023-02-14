use crate::ingestion_types::{EthConfig, KafkaConfig, LocalStorage, S3Storage, SnowflakeConfig};
use serde::{
    de::Deserializer,
    ser::{self, Serializer},
};
use serde::{Deserialize, Serialize};

use prettytable::Table;
use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    str::FromStr,
};

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]

pub struct Connection {
    #[prost(oneof = "Authentication", tags = "1,2,3,4,5")]
    /// authentication config - depends on db_type
    pub authentication: Option<Authentication>,
    #[prost(string, optional, tag = "6")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[prost(string, optional, tag = "7")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_id: Option<String>,
    #[prost(enumeration = "DBType", tag = "8")]
    #[serde(serialize_with = "serialize_db_type_i32_as_string")]
    #[serde(deserialize_with = "deserialize_db_type_str_as_i32")]
    /// database type - posible values could be: `Postgres`, `Snowflake`, `Ethereum`, `Events`, `Kafka`.; Type: String
    pub db_type: i32,
    #[prost(string, tag = "9")]
    pub name: String,
}
fn serialize_db_type_i32_as_string<S>(input: &i32, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let db_type = DBType::try_from(input.to_owned()).map_err(ser::Error::custom)?;
    serializer.serialize_str(db_type.as_str_name())
}

fn deserialize_db_type_str_as_i32<'de, D>(deserializer: D) -> Result<i32, D::Error>
where
    D: Deserializer<'de>,
{
    let db_type_string = String::deserialize(deserializer)?;
    let db_type = DBType::from_str(&db_type_string).map_err(serde::de::Error::custom)?;
    Ok(db_type as i32)
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Enumeration)]
#[repr(i32)]
pub enum DBType {
    Postgres = 0,
    Snowflake = 1,
    Ethereum = 2,
    Events = 3,
    Kafka = 4,
    ObjectStore = 5,
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
            5 => Ok(DBType::ObjectStore),
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
            DBType::ObjectStore => "object_store",
        }
    }
}
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
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

impl PostgresAuthentication {
    pub fn convert_to_table(&self) -> Table {
        table!(
            ["user", self.user.as_str()],
            ["password", "*************"],
            ["host", self.host],
            ["port", self.port],
            ["database", self.database]
        )
    }
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct EventsAuthentication {}
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Oneof, Hash)]
pub enum Authentication {
    #[prost(message, tag = "1")]
    /// In yaml, present as tag: `!Postgres`
    Postgres(PostgresAuthentication),
    #[prost(message, tag = "2")]
    /// In yaml, present as tag: `!Ethereum`
    Ethereum(EthConfig),
    /// In yaml, present as tag: `!Events`
    #[prost(message, tag = "3")]
    Events(EventsAuthentication),
    #[prost(message, tag = "4")]
    /// In yaml, present as tag: `!Snowflake`
    Snowflake(SnowflakeConfig),
    #[prost(message, tag = "5")]
    /// In yaml, present as tag: `!Kafka`
    Kafka(KafkaConfig),
    #[prost(message, tag = "6")]
    /// In yaml, present as tag: `!ObjectStore`
    S3Storage(S3Storage),
    #[prost(message, tag = "7")]
    /// In yaml, present as tag: `!ObjectStore`
    LocalStorage(LocalStorage),
}

impl Default for Authentication {
    fn default() -> Self {
        Authentication::Postgres(PostgresAuthentication::default())
    }
}

impl Display for DBType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{self:?}")
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
            "ObjectStore" | "objectStore" | "object_store" => Ok(DBType::ObjectStore),
            _ => Err("Not match any value in Enum DBType"),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq, ::prost::Message)]
pub struct AuthenticationWrapper {
    #[prost(oneof = "Authentication", tags = "1,2,3,4,5")]
    pub authentication: Option<Authentication>,
}

impl From<AuthenticationWrapper> for Authentication {
    fn from(input: AuthenticationWrapper) -> Self {
        input.authentication.unwrap_or_default()
    }
}
