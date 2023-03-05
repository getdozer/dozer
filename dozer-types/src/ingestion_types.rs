use prettytable::Table as PrettyTable;
use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{errors::internal::BoxedError, node::OpIdentifier, types::Operation};

#[derive(Debug, Clone, PartialEq)]
pub struct IngestionMessage {
    pub identifier: OpIdentifier,
    pub kind: IngestionMessageKind,
}

impl IngestionMessage {
    pub fn new_op(txn: u64, seq_no: u64, op: Operation) -> Self {
        Self {
            identifier: OpIdentifier::new(txn, seq_no),
            kind: IngestionMessageKind::OperationEvent(op),
        }
    }

    pub fn new_snapshotting_done(txn: u64, seq_no: u64) -> Self {
        Self {
            identifier: OpIdentifier::new(txn, seq_no),
            kind: IngestionMessageKind::SnapshottingDone,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum IngestionMessageKind {
    OperationEvent(Operation),
    SnapshottingDone,
}

#[derive(Error, Debug)]
pub enum IngestorError {
    #[error("Failed to send message on channel")]
    ChannelError(#[from] BoxedError),
}

pub trait IngestorForwarder: Send + Sync + Debug {
    fn forward(&self, msg: IngestionMessage) -> Result<(), IngestorError>;
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
pub struct GrpcConfig {
    #[prost(string, tag = "1", default = "0.0.0.0")]
    #[serde(default = "default_ingest_host")]
    pub host: String,
    #[prost(uint32, tag = "2", default = "8085")]
    #[serde(default = "default_ingest_port")]
    pub port: u32,
    #[prost(oneof = "GrpcConfigSchemas", tags = "3,4")]
    pub schemas: Option<GrpcConfigSchemas>,
    #[prost(string, tag = "5", default = "default")]
    #[serde(default = "default_grpc_adapter")]
    pub adapter: String,
}

fn default_grpc_adapter() -> String {
    "default".to_owned()
}

fn default_ingest_host() -> String {
    "0.0.0.0".to_owned()
}

fn default_ingest_port() -> u32 {
    8085
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Oneof, Hash)]
pub enum GrpcConfigSchemas {
    #[prost(string, tag = "3")]
    Inline(String),
    #[prost(string, tag = "4")]
    Path(String),
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct EthConfig {
    #[prost(oneof = "EthProviderConfig", tags = "2,3")]
    pub provider: Option<EthProviderConfig>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Oneof, Hash)]
pub enum EthProviderConfig {
    #[prost(message, tag = "2")]
    Log(EthLogConfig),
    #[prost(message, tag = "3")]
    Trace(EthTraceConfig),
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct EthLogConfig {
    #[prost(string, tag = "1")]
    pub wss_url: String,
    #[prost(message, optional, tag = "2")]
    pub filter: Option<EthFilter>,
    #[prost(message, repeated, tag = "3")]
    #[serde(default)]
    pub contracts: Vec<EthContract>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct EthTraceConfig {
    #[prost(string, tag = "1")]
    pub https_url: String,
    // Starting block
    #[prost(uint64, tag = "2")]
    pub from_block: u64,
    #[prost(uint64, optional, tag = "3")]
    pub to_block: Option<u64>,
    #[prost(uint64, tag = "4", default = "3")]
    #[serde(default = "default_batch_size")]
    pub batch_size: u64,
}

fn default_batch_size() -> u64 {
    3
}

impl EthConfig {
    pub fn convert_to_table(&self) -> PrettyTable {
        let mut table = table!();

        let provider = self.provider.as_ref().expect("Must provide provider");
        match provider {
            EthProviderConfig::Log(log) => {
                table.add_row(row!["provider", "logs"]);
                table.add_row(row!["wss_url", format!("{:?}", log.wss_url)]);
                if let Some(filter) = &log.filter {
                    table.add_row(row!["filter", format!("{filter:?}")]);
                }
                if !log.contracts.is_empty() {
                    table.add_row(row!["contracts", format!("{:?}", log.contracts)]);
                }
            }
            EthProviderConfig::Trace(trace) => {
                table.add_row(row!["https_url", format!("{:?}", trace.https_url)]);
                table.add_row(row!["provider", "traces"]);
                table.add_row(row!("trace", format!("{trace:?}")));
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

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct DeltaTable {
    #[prost(string, tag = "1")]
    pub path: String,
    #[prost(string, tag = "2")]
    pub name: String

}

impl DeltaTable {
    pub fn convert_to_table(&self) -> PrettyTable {
        todo!()
    }
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct DeltaLakeConfig {
    #[prost(message, repeated, tag = "1")]
    pub tables: Vec<DeltaTable>,
}
