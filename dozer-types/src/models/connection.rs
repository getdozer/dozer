use crate::ingestion_types::{
    DeltaLakeConfig, EthConfig, GrpcConfig, KafkaConfig, LocalStorage, S3Storage, SnowflakeConfig,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;

use prettytable::Table;
use regex::Regex;
use tokio_postgres::config::SslMode;

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct Connection {
    #[prost(oneof = "ConnectionConfig", tags = "1,2,3,4,5,6,7,8")]
    /// authentication config - depends on db_type
    pub config: Option<ConnectionConfig>,
    #[prost(string, tag = "9")]
    pub name: String,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct PostgresConfig {
    #[prost(string, optional, tag = "1")]
    pub user: Option<String>,
    #[prost(string, optional, tag = "2")]
    pub password: Option<String>,
    #[prost(string, optional, tag = "3")]
    pub host: Option<String>,
    #[prost(uint32, optional, tag = "4")]
    pub port: Option<u32>,
    #[prost(string, optional, tag = "5")]
    pub database: Option<String>,
    #[prost(string, optional, tag = "6")]
    pub sslmode: Option<String>,
    #[prost(string, optional, tag = "7")]
    pub connection_url: Option<String>,
}

#[derive(Eq, PartialEq, Clone, Debug, Hash)]
pub struct PostgresConfigReplenished {
    pub user: String,
    pub password: String,
    pub host: String,
    pub port: u32,
    pub database: String,
    pub ssl_mode: SslMode,
}

impl PostgresConfigReplenished {
    pub fn convert_to_table(&self) -> Table {
        table!(
            ["user", self.user],
            ["password", "*************"],
            ["host", self.host],
            ["port", self.port],
            ["database", self.database],
            ["sslmode", format!("{:?}", self.ssl_mode)]
        )
    }
}

impl PostgresConfig {
    pub fn replenish(&self) -> PostgresConfigReplenished {
        if self.connection_url.is_none() {
            PostgresConfigReplenished {
                user: self.user.clone().unwrap(),
                password: self.password.clone().unwrap(),
                host: self.host.clone().unwrap(),
                port: self.port.unwrap(),
                database: self.database.clone().unwrap(),
                ssl_mode: get_ssl_mode(self.sslmode.clone()),
            }
        } else {
            let map = connection_url_map(self.connection_url.as_ref().unwrap(), self);
            PostgresConfigReplenished {
                user: map
                    .get("user")
                    .expect("user is missing from connection url")
                    .to_string(),
                password: map
                    .get("password")
                    .expect("password is missing from connection url")
                    .to_string(),
                host: map
                    .get("host")
                    .expect("host is missing from connection url")
                    .to_string(),
                port: u32::from_str(
                    map.get("port")
                        .expect("port is missing from connection url"),
                )
                .unwrap(),
                database: map
                    .get("database")
                    .expect("database is missing from connection url")
                    .to_string(),
                ssl_mode: get_ssl_mode(map.get("sslmode").cloned()),
            }
        }
    }
}

fn get_ssl_mode(mode: Option<String>) -> SslMode {
    match mode {
        Some(m) => match m.as_str() {
            "disable" | "Disable" | "" => SslMode::Disable,
            "prefer" | "Prefer" => SslMode::Prefer,
            "require" | "Require" => SslMode::Require,
            &_ => SslMode::Disable,
        },
        None => SslMode::Disable,
    }
}

pub fn connection_url_map(url: &str, config: &PostgresConfig) -> HashMap<String, String> {
    let re = Regex::new(r"(?P<protocol>[^/\s]+)?(:/{2})((?P<user>[^:\s]+)?:{1}(?P<password>[^@\s]+)?@{1}){0,1}(?P<host>[^:\s]+)?:{1}(?P<port>[^/\s]+)?(?P<db>/{1}(?P<database>[^?/\s]+)?)+\?{0,1}(?P<arg>[^\s]+)?").unwrap();
    let matches = re.captures(url);
    let mut entities = HashMap::new();
    if let Some(cap) = matches {
        entities.insert(
            String::from("protocol"),
            cap.name("protocol").unwrap().as_str().to_string(),
        );
        if cap.name("user").is_none() && config.user.is_some() {
            entities.insert(
                String::from("user"),
                config.user.clone().unwrap().as_str().to_string(),
            );
        } else if cap.name("user").is_some() && config.user.is_none() {
            entities.insert(
                String::from("user"),
                cap.name("user").unwrap().as_str().to_string(),
            );
        }
        if cap.name("password").is_none() && config.password.is_some() {
            entities.insert(
                String::from("password"),
                config.password.clone().unwrap().as_str().to_string(),
            );
        } else if cap.name("password").is_some() && config.password.is_none() {
            entities.insert(
                String::from("password"),
                cap.name("password").unwrap().as_str().to_string(),
            );
        }
        if cap.name("host").is_none() && config.host.is_some() {
            entities.insert(
                String::from("host"),
                config.host.clone().unwrap().as_str().to_string(),
            );
        } else if cap.name("host").is_some() && config.host.is_none() {
            entities.insert(
                String::from("host"),
                cap.name("host").unwrap().as_str().to_string(),
            );
        }
        if cap.name("port").is_none() && config.port.is_some() {
            entities.insert(String::from("port"), config.port.unwrap().to_string());
        } else if cap.name("port").is_some() && config.port.is_none() {
            entities.insert(
                String::from("port"),
                cap.name("port").unwrap().as_str().to_string(),
            );
        }
        if cap.name("database").is_none() && config.database.is_some() {
            entities.insert(
                String::from("database"),
                config.database.clone().unwrap().as_str().to_string(),
            );
        } else if cap.name("database").is_some() && config.database.is_none() {
            entities.insert(
                String::from("database"),
                cap.name("database").unwrap().as_str().to_string(),
            );
        }

        if let Some(arg) = cap.name("arg") {
            let sp = arg.as_str().split(',');
            for s in sp {
                if let Some((key, val)) = s.split_once('=') {
                    if key == "ssl-mode" {
                        entities.insert(key.to_string(), val.to_string());
                    }
                }
            }
        }
    }

    entities
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Oneof, Hash)]
pub enum ConnectionConfig {
    #[prost(message, tag = "1")]
    /// In yaml, present as tag: `!Postgres`
    Postgres(PostgresConfig),
    #[prost(message, tag = "2")]
    /// In yaml, present as tag: `!Ethereum`
    Ethereum(EthConfig),
    /// In yaml, present as tag: `!Grpc`
    #[prost(message, tag = "3")]
    Grpc(GrpcConfig),
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
    #[prost(message, tag = "8")]
    /// In yaml, present as tag" `!DeltaLake`
    DeltaLake(DeltaLakeConfig),
}
