use prettytable::Table as PrettyTable;
use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    errors::internal::BoxedError,
    types::{Commit, Operation},
};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum IngestionMessage {
    Begin(),
    OperationEvent(Operation),
    Commit(Commit),
}

#[derive(Error, Debug)]
pub enum IngestorError {
    #[error("Failed to send message on channel")]
    ChannelError(#[from] BoxedError),
}

pub trait IngestorForwarder: Send + Sync + Debug {
    fn forward(&self, msg: ((u64, u64), Operation)) -> Result<(), IngestorError>;
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct EthFilter {
    // Starting block
    #[prost(uint64, optional, tag = "1")]
    pub from_block: Option<u64>,
    #[prost(uint64, optional, tag = "2")]
    pub to_block: Option<u64>,
    #[prost(string, repeated, tag = "3")]
    #[serde(default)]
    pub addresses: Vec<String>,
    #[prost(string, repeated, tag = "4")]
    #[serde(default)]
    pub topics: Vec<String>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct EthConfig {
    #[prost(string, tag = "1")]
    pub wss_url: String,
    #[prost(oneof = "EthProvider", tags = "2,3")]
    pub provider: Option<EthProvider>,
}

impl Default for EthProvider {
    fn default() -> Self {
        EthProvider::Log(EthLogConfig::default())
    }
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Oneof, Hash)]
pub enum EthProvider {
    #[prost(message, tag = "2")]
    Log(EthLogConfig),
    #[prost(message, tag = "3")]
    Trace(EthTraceConfig),
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct EthLogConfig {
    #[prost(message, optional, tag = "1")]
    pub filter: Option<EthFilter>,
    #[prost(message, repeated, tag = "2")]
    #[serde(default)]
    pub contracts: Vec<EthContract>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct EthTraceConfig {
    // Starting block
    #[prost(uint64, tag = "1")]
    pub from_block: u64,
    #[prost(uint64, optional, tag = "2")]
    pub to_block: Option<u64>,
}

impl EthConfig {
    pub fn convert_to_table(&self) -> PrettyTable {
        let mut table = table!(["wss_url", self.wss_url]);

        debug_assert!(self.provider.is_some());
        let provider = self.provider.as_ref().unwrap();
        match provider {
            EthProvider::Log(log) => {
                table.add_row(row!["provider", "logs"]);
                if let Some(filter) = &log.filter {
                    table.add_row(row!["filter", format!("{:?}", filter)]);
                }
                if log.contracts.len() > 0 {
                    table.add_row(row!["contracts", format!("{:?}", log.contracts)]);
                }
            }
            EthProvider::Trace(trace) => {
                table.add_row(row!["provider", "traces"]);
                table.add_row(row!("trace", format!("{:?}", trace)));
            }
        }
        table
    }
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct EthContract {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub address: String,
    #[prost(string, tag = "3")]
    pub abi: String,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct KafkaConfig {
    #[prost(string, tag = "1")]
    pub broker: String,
    #[prost(string, optional, tag = "3")]
    pub schema_registry_url: Option<String>,
}

impl KafkaConfig {
    pub fn convert_to_table(&self) -> PrettyTable {
        table!(
            ["broker", self.broker],
            [
                "schema registry url",
                self.schema_registry_url
                    .as_ref()
                    .map_or("--------", |url| url)
            ]
        )
    }
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct SnowflakeConfig {
    #[prost(string, tag = "1")]
    pub server: String,
    #[prost(string, tag = "2")]
    pub port: String,
    #[prost(string, tag = "3")]
    pub user: String,
    #[prost(string, tag = "4")]
    pub password: String,
    #[prost(string, tag = "5")]
    pub database: String,
    #[prost(string, tag = "6")]
    pub schema: String,
    #[prost(string, tag = "7")]
    pub warehouse: String,
    #[prost(string, optional, tag = "8")]
    pub driver: Option<String>,
}

impl SnowflakeConfig {
    pub fn convert_to_table(&self) -> PrettyTable {
        table!(
            ["server", self.server],
            ["port", self.port],
            ["user", self.user],
            ["password", "************"],
            ["database", self.database],
            ["schema", self.schema],
            ["warehouse", self.warehouse],
            ["driver", self.driver.as_ref().map_or("default", |d| d)]
        )
    }
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct DataFusionConfig {
    #[prost(string, tag = "1")]
    pub access_key_id: String,
    #[prost(string, tag = "2")]
    pub secret_access_key: String,
    #[prost(string, tag = "3")]
    pub region: String,
    #[prost(string, tag = "4")]
    pub bucket_name: String,
}

impl DataFusionConfig {
    pub fn convert_to_table(&self) -> PrettyTable {
        table!(
            ["access_key_id", self.access_key_id],
            ["secret_access_key", self.secret_access_key],
            ["region", self.region],
            ["bucket_name", self.bucket_name]
        )
    }
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct Table {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub prefix: String,
    #[prost(string, tag = "3")]
    pub file_type: String,
    #[prost(string, tag = "4")]
    pub extension: String,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct S3Details {
    #[prost(string, tag = "1")]
    pub access_key_id: String,
    #[prost(string, tag = "2")]
    pub secret_access_key: String,
    #[prost(string, tag = "3")]
    pub region: String,
    #[prost(string, tag = "4")]
    pub bucket_name: String,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct S3Storage {
    #[prost(message, optional, tag = "1")]
    pub details: Option<S3Details>,
    #[prost(message, repeated, tag = "2")]
    pub tables: Vec<Table>,
}

impl S3Storage {
    pub fn convert_to_table(&self) -> PrettyTable {
        self.details.as_ref().map_or_else(
            || table!(),
            |details| {
                table!(
                    ["access_key_id", details.access_key_id],
                    ["secret_access_key", details.secret_access_key],
                    ["region", details.region],
                    ["bucket_name", details.bucket_name]
                )
            },
        )
    }
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct LocalDetails {
    #[prost(string, tag = "1")]
    pub path: String,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct LocalStorage {
    #[prost(message, optional, tag = "1")]
    pub details: Option<LocalDetails>,
    #[prost(message, repeated, tag = "2")]
    pub tables: Vec<Table>,
}

impl LocalStorage {
    pub fn convert_to_table(&self) -> PrettyTable {
        self.details
            .as_ref()
            .map_or_else(|| table!(), |details| table!(["path", details.path]))
    }
}
