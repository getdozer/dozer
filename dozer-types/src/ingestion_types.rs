use prettytable::Table as PrettyTable;
use schemars::JsonSchema;
use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::{models::api_config::AppGrpcOptions, node::OpIdentifier, types::Operation};

#[derive(Clone, Debug, PartialEq)]
/// All possible kinds of `IngestionMessage`.
pub enum IngestionMessage {
    /// A CDC event.
    OperationEvent {
        /// Index of the table that the event belongs to.
        table_index: usize,
        /// The CDC event.
        op: Operation,
        /// If this connector supports restarting from a specific CDC event, it should provide an identifier.
        id: Option<OpIdentifier>,
    },
    /// A connector uses this message kind to notify Dozer that a initial snapshot of the source tables is started
    SnapshottingStarted,
    /// A connector uses this message kind to notify Dozer that a initial snapshot of the source tables is done,
    /// and the data is up-to-date until next CDC event.
    SnapshottingDone,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema, Default)]
pub struct EthFilter {
    // Starting block
    pub from_block: Option<u64>,

    pub to_block: Option<u64>,

    #[serde(default)]
    pub addresses: Vec<String>,

    #[serde(default)]
    pub topics: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema, Default)]
pub struct GrpcConfig {
    #[serde(default = "default_ingest_host")]
    pub host: String,

    #[serde(default = "default_ingest_port")]
    pub port: u32,

    pub schemas: Option<GrpcConfigSchemas>,

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

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub enum GrpcConfigSchemas {
    Inline(String),

    Path(String),
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema, Default)]
pub struct EthConfig {
    pub provider: Option<EthProviderConfig>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub enum EthProviderConfig {
    Log(EthLogConfig),

    Trace(EthTraceConfig),
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema, Default)]
pub struct EthLogConfig {
    pub wss_url: String,

    pub filter: Option<EthFilter>,

    #[serde(default)]
    pub contracts: Vec<EthContract>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema, Default)]
pub struct EthTraceConfig {
    pub https_url: String,
    // Starting block
    pub from_block: u64,

    pub to_block: Option<u64>,

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

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub struct EthContract {
    pub name: String,

    pub address: String,

    pub abi: String,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub struct KafkaConfig {
    pub broker: String,

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

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub struct SnowflakeConfig {
    pub server: String,

    pub port: String,

    pub user: String,

    pub password: String,

    pub database: String,

    pub schema: String,

    pub warehouse: String,

    pub driver: Option<String>,

    pub role: String,
}

impl SnowflakeConfig {
    pub fn convert_to_table(&self) -> PrettyTable {
        table!(
            ["server", self.server],
            ["port", self.port],
            ["user", self.user],
            ["password", "************"],
            ["role", self.role],
            ["database", self.database],
            ["schema", self.schema],
            ["warehouse", self.warehouse],
            ["driver", self.driver.as_ref().map_or("default", |d| d)]
        )
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub struct DataFusionConfig {
    pub access_key_id: String,

    pub secret_access_key: String,

    pub region: String,

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

// #[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone,  Hash, JsonSchema)]
// pub struct Table {
//
//     pub name: String,
//
//     pub prefix: String,
//
//     pub file_type: String,
//
//     pub extension: String,
// }

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub struct Table {
    pub config: Option<TableConfig>,

    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub enum TableConfig {
    CSV(CsvConfig),

    Delta(DeltaConfig),

    Parquet(ParquetConfig),
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub struct CsvConfig {
    pub path: String,

    pub extension: String,

    #[serde(default = "default_false")]
    pub marker_file: bool,

    #[serde(default = "default_marker")]
    pub marker_extension: String,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub struct DeltaConfig {
    pub path: String,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub struct ParquetConfig {
    pub path: String,

    pub extension: String,

    #[serde(default = "default_false")]
    pub marker_file: bool,

    #[serde(default = "default_marker")]
    pub marker_extension: String,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub struct S3Details {
    pub access_key_id: String,

    pub secret_access_key: String,

    pub region: String,

    pub bucket_name: String,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub struct S3Storage {
    pub details: Option<S3Details>,

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

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub struct LocalDetails {
    pub path: String,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub struct LocalStorage {
    pub details: Option<LocalDetails>,

    pub tables: Vec<Table>,
}

impl LocalStorage {
    pub fn convert_to_table(&self) -> PrettyTable {
        self.details
            .as_ref()
            .map_or_else(|| table!(), |details| table!(["path", details.path]))
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub struct DeltaTable {
    pub path: String,

    pub name: String,
}

impl DeltaTable {
    pub fn convert_to_table(&self) -> PrettyTable {
        todo!()
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub struct DeltaLakeConfig {
    pub tables: Vec<DeltaTable>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub struct MongodbConfig {
    pub connection_string: String,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub struct MySQLConfig {
    pub url: String,

    pub server_id: Option<u32>,
}

fn default_false() -> bool {
    false
}

fn default_marker() -> String {
    String::from(".marker")
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub struct NestedDozerConfig {
    pub grpc: Option<AppGrpcOptions>,

    pub log_options: Option<NestedDozerLogOptions>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub struct NestedDozerLogOptions {
    #[serde(default = "default_log_batch_size")]
    pub batch_size: u32,

    #[serde(default = "default_timeout")]
    pub timeout_in_millis: u32,

    #[serde(default = "default_buffer_size")]
    pub buffer_size: u32,
}

fn default_log_batch_size() -> u32 {
    30
}
fn default_timeout() -> u32 {
    1000
}

fn default_buffer_size() -> u32 {
    1000
}

pub fn default_log_options() -> NestedDozerLogOptions {
    NestedDozerLogOptions {
        batch_size: default_log_batch_size(),
        timeout_in_millis: default_timeout(),
        buffer_size: default_buffer_size(),
    }
}
