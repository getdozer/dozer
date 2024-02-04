use crate::models::ingestion_types::{
    DeltaLakeConfig, EthConfig, GrpcConfig, JavaScriptConfig, KafkaConfig, LocalStorage,
    MongodbConfig, MySQLConfig, NestedDozerConfig, S3Storage, SnowflakeConfig, WebhookConfig,
    SECRET,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::errors::types::DeserializationError;
use crate::errors::types::DeserializationError::{
    InvalidConnectionUrl, MismatchingFieldInPostgresConfig, MissingFieldInPostgresConfig,
    UnableToParseConnectionUrl, UnknownSslMode,
};
use prettytable::Table;
use tokio_postgres::config::{Host, SslMode};
use tokio_postgres::Config;

use super::ingestion_types::OracleConfig;

pub trait SchemaExample {
    fn example() -> Self;
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Connection {
    pub config: ConnectionConfig,
    pub name: String,
}

/// Configuration for a Postgres connection
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
#[schemars(example = "Self::example")]
pub struct PostgresConfig {
    /// The username to use for authentication
    pub user: Option<String>,

    /// The password to use for authentication
    pub password: Option<String>,

    /// The host to connect to (IP or DNS name)
    pub host: Option<String>,

    /// The port to connect to (default: 5432)
    pub port: Option<u32>,

    /// The database to connect to (default: postgres)
    pub database: Option<String>,

    /// The sslmode to use for the connection (disable, prefer, require)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sslmode: Option<String>,

    /// The connection url to use
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_url: Option<String>,

    /// The schema of the tables
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,

    /// The snapshot batch size
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<u32>,
}

impl SchemaExample for PostgresConfig {
    fn example() -> Self {
        Self {
            user: Some("postgres".to_string()),
            password: Some("postgres".to_string()),
            host: Some("localhost".to_string()),
            port: Some(5432),
            database: Some("postgres".to_string()),
            schema: Some("public".to_string()),
            ..Default::default()
        }
    }
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
            ["password", SECRET],
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
                        Some(host) => match host {
                            Host::Tcp(host) => host.clone(),
                            #[cfg(unix)]
                            Host::Unix(path) => path.to_string_lossy().to_string(),
                        },
                        None => String::new(),
                    },
                    "port" => match val.get_ports().first() {
                        Some(p) => p.to_string(),
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

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone, Hash)]
pub struct AerospikeConnection {
    pub hosts: String,
    pub namespace: String,
    pub sets: Vec<String>,
    #[serde(default)]
    pub batching: bool,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
#[serde(deny_unknown_fields)]
pub enum ConnectionConfig {
    /// In yaml, present as tag: `!Postgres`
    Postgres(PostgresConfig),

    /// In yaml, present as tag: `!Ethereum`
    Ethereum(EthConfig),

    /// In yaml, present as tag: `!Grpc`
    Grpc(GrpcConfig),

    /// In yaml, present as tag: `!Snowflake`
    Snowflake(SnowflakeConfig),

    /// In yaml, present as tag: `!Kafka`
    Kafka(KafkaConfig),

    /// In yaml, present as tag: `!ObjectStore`
    S3Storage(S3Storage),

    /// In yaml, present as tag: `!ObjectStore`
    LocalStorage(LocalStorage),

    /// In yaml, present as tag" `!DeltaLake`
    DeltaLake(DeltaLakeConfig),

    /// In yaml, present as tag: `!MongoDB`
    MongoDB(MongodbConfig),

    /// In yaml, present as tag" `!MySQL`
    MySQL(MySQLConfig),

    /// In yaml, present as tag" `!Dozer`
    Dozer(NestedDozerConfig),

    /// In yaml, present as tag" `!JavaScript`
    JavaScript(JavaScriptConfig),

    /// In yaml, present as tag" `!Webhook`
    Webhook(WebhookConfig),

    Oracle(OracleConfig),
    Aerospike(AerospikeConnection),
}

impl ConnectionConfig {
    pub fn get_type_name(&self) -> String {
        match self {
            ConnectionConfig::Postgres(_) => "postgres".to_string(),
            ConnectionConfig::Ethereum(_) => "ethereum".to_string(),
            ConnectionConfig::Grpc(_) => "grpc".to_string(),
            ConnectionConfig::Snowflake(_) => "snowflake".to_string(),
            ConnectionConfig::Kafka(_) => "kafka".to_string(),
            ConnectionConfig::S3Storage(_) => "s3storage".to_string(),
            ConnectionConfig::LocalStorage(_) => "localstorage".to_string(),
            ConnectionConfig::DeltaLake(_) => "deltalake".to_string(),
            ConnectionConfig::MongoDB(_) => "mongodb".to_string(),
            ConnectionConfig::MySQL(_) => "mysql".to_string(),
            ConnectionConfig::Dozer(_) => "dozer".to_string(),
            ConnectionConfig::JavaScript(_) => "javascript".to_string(),
            ConnectionConfig::Webhook(_) => "webhook".to_string(),
            ConnectionConfig::Oracle(_) => "oracle".to_string(),
            ConnectionConfig::Aerospike(_) => "aerospike".to_string(),
        }
    }
}
