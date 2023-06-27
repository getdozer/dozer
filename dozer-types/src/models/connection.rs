use crate::ingestion_types::{
    DeltaLakeConfig, EthConfig, GrpcConfig, KafkaConfig, LocalStorage, S3Storage, SnowflakeConfig,
};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::errors::types::DeserializationError;
use crate::errors::types::DeserializationError::{
    InvalidConnectionUrl, MismatchingFieldInPostgresConfig, MissingFieldInPostgresConfig,
    UnableToParseConnectionUrl, UnknownSslMode,
};
use prettytable::Table;
use tokio_postgres::config::SslMode;
use tokio_postgres::{Config};

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
    pub sslmode: SslMode,
}

impl PostgresConfigReplenished {
    pub fn convert_to_table(&self) -> Table {
        table!(
            ["user", self.user],
            ["password", "*************"],
            ["host", self.host],
            ["port", self.port],
            ["database", self.database],
            ["sslmode", format!("{:?}", self.sslmode)]
        )
    }
}

impl PostgresConfig {
    pub fn replenish(&self) -> Result<PostgresConfigReplenished, DeserializationError> {
        Ok(PostgresConfigReplenished {
            user: self.lookup("user")?,
            password: self.lookup("password")?,
            host: self.lookup("host")?,
            port: u32::from_str(self.lookup("port")?.as_str())
                .map_err(UnableToParseConnectionUrl)?,
            database: self.lookup("database")?,
            sslmode: get_sslmode(self.lookup("sslmode")?)?,
        })
    }

    fn lookup(&self, field: &str) -> Result<String, DeserializationError> {
        let connection_url_val: String = match self.connection_url.clone() {
            Some(url) => {
                let val = Config::from_str(url.as_str()).map_err(InvalidConnectionUrl)?;
                match field {
                    "user" => match val.get_user() {
                        Some(usr) => usr.to_string(),
                        None => String::new(),
                    },
                    "password" => match val.get_password() {
                        Some(pw) => String::from_utf8(pw.to_owned()).unwrap(),
                        None => String::new(),
                    },
                    "host" => match val.get_hosts().first() {
                        Some(h) => format!("{:?}", h),
                        None => String::new(),
                    },
                    "port" => match val.get_ports().first() {
                        Some(p) => format!("{:?}", p),
                        None => String::new(),
                    },
                    "database" => match val.get_dbname() {
                        Some(db) => db.to_string(),
                        None => String::new(),
                    },
                    "sslmode" => format!("{:?}", val.get_ssl_mode()),
                    &_ => String::new(),
                }
            }
            None => String::new(),
        };
        let field_val = match field {
            "user" => match self.user.clone() {
                Some(usr) => usr,
                None => String::new(),
            },
            "password" => match self.password.clone() {
                Some(pw) => pw,
                None => String::new(),
            },
            "host" => match self.host.clone() {
                Some(h) => h,
                None => String::new(),
            },
            "port" => match self.port {
                Some(p) => p.to_string(),
                None => String::new(),
            },
            "database" => match self.database.clone() {
                Some(db) => db,
                None => String::new(),
            },
            "sslmode" => match self.sslmode.clone() {
                Some(ssl) => ssl,
                None => String::new(),
            },
            &_ => String::new(),
        };
        if connection_url_val.is_empty() && field_val.is_empty() {
            if field == "sslmode" {
                Ok(format!("{:?}", SslMode::Disable))
            } else {
                Err(MissingFieldInPostgresConfig(field.to_string()))
            }
        } else if !connection_url_val.is_empty() && field_val.is_empty() {
            Ok(connection_url_val)
        } else if connection_url_val.is_empty() && !field_val.is_empty() {
            Ok(field_val)
        } else if !connection_url_val.is_empty()
            && !field_val.is_empty()
            && connection_url_val == field_val
        {
            Ok(connection_url_val)
        } else {
            Err(MismatchingFieldInPostgresConfig(field.to_string()))
        }
    }
}

fn get_sslmode(mode: String) -> Result<SslMode, DeserializationError> {
    match mode.as_str() {
        "disable" | "Disable" => Ok(SslMode::Disable),
        "prefer" | "Prefer" => Ok(SslMode::Prefer),
        "require" | "Require" => Ok(SslMode::Require),
        &_ => Err(UnknownSslMode(mode)),
    }
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
