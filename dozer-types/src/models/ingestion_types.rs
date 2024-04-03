use prettytable::Table as PrettyTable;
use schemars::JsonSchema;
use std::{fmt::Debug, time::Duration};

use serde::{Deserialize, Serialize};

use crate::{
    epoch::SourceTime,
    helper::{deserialize_duration_secs_f64, f64_schema, serialize_duration_secs_f64},
    models::connection::SchemaExample,
    node::OpIdentifier,
    types::Operation,
};

use super::equal_default;

pub const SECRET: &str = "*********";

#[derive(Clone, Debug, PartialEq)]
/// All possible kinds of `IngestionMessage`.
pub enum IngestionMessage {
    /// A CDC event.
    OperationEvent {
        /// Index of the table that the event belongs to.
        table_index: usize,
        /// The CDC event.
        op: Operation,
        /// If this connector supports restarting from a specific CDC event, it should provide a `OpIdentifier`.
        id: Option<OpIdentifier>,
    },
    TransactionInfo(TransactionInfo),
}

#[derive(Clone, Debug, PartialEq)]
pub enum TransactionInfo {
    Commit {
        /// If this connector supports restarting from after this commit, it should provide a `OpIdentifier`.
        id: Option<OpIdentifier>,
        source_time: Option<SourceTime>,
    },
    /// A connector uses this message kind to notify Dozer that a initial snapshot of the source tables is started
    SnapshottingStarted,
    /// A connector uses this message kind to notify Dozer that a initial snapshot of the source tables is done,
    /// and the data is up-to-date until next CDC event.
    SnapshottingDone { id: Option<OpIdentifier> },
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema, Default)]
pub struct EthFilter {
    // Starting block
    pub from_block: Option<u64>,

    pub to_block: Option<u64>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub addresses: Vec<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub topics: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
#[schemars(example = "Self::example")]

pub struct GrpcConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u32>,

    pub schemas: ConfigSchemas,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub adapter: Option<String>,
}

pub fn default_grpc_adapter() -> String {
    "default".to_owned()
}

pub fn default_ingest_host() -> String {
    "0.0.0.0".to_owned()
}

pub fn default_ingest_port() -> u32 {
    8085
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub enum ConfigSchemas {
    Inline(String),
    Path(String),
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
#[schemars(example = "Self::example")]

pub struct EthConfig {
    pub provider: EthProviderConfig,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub enum EthProviderConfig {
    Log(EthLogConfig),

    Trace(EthTraceConfig),
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub struct EthLogConfig {
    pub wss_url: String,

    pub filter: Option<EthFilter>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub contracts: Vec<EthContract>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub struct EthTraceConfig {
    pub https_url: String,
    // Starting block
    pub from_block: u64,

    pub to_block: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<u64>,
}

pub fn default_batch_size() -> u64 {
    3
}

impl EthConfig {
    pub fn convert_to_table(&self) -> PrettyTable {
        let mut table = table!();

        match &self.provider {
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
#[schemars(example = "Self::example")]

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
#[schemars(example = "Self::example")]

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

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_duration_secs_f64",
        serialize_with = "serialize_duration_secs_f64"
    )]
    #[schemars(schema_with = "f64_schema")]
    pub poll_interval_seconds: Option<Duration>,
}

impl SnowflakeConfig {
    pub fn convert_to_table(&self) -> PrettyTable {
        table!(
            ["server", self.server],
            ["port", self.port],
            ["user", self.user],
            ["password", SECRET],
            ["role", self.role],
            ["database", self.database],
            ["schema", self.schema],
            ["warehouse", self.warehouse],
            ["driver", self.driver.as_ref().map_or("default", |d| d)],
            [
                "poll_interval_seconds",
                format!(
                    "{}s",
                    self.poll_interval_seconds
                        .unwrap_or_else(default_snowflake_poll_interval)
                        .as_secs_f64()
                )
            ]
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
            ["access_key_id", SECRET],
            ["secret_access_key", SECRET],
            ["region", self.region],
            ["bucket_name", self.bucket_name]
        )
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub struct Table {
    pub config: TableConfig,

    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub enum TableConfig {
    CSV(CsvConfig),
    Parquet(ParquetConfig),
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub struct CsvConfig {
    pub path: String,

    pub extension: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub marker_extension: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub struct ParquetConfig {
    pub path: String,

    pub extension: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub marker_extension: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub struct S3Details {
    pub access_key_id: String,

    pub secret_access_key: String,

    pub region: String,

    pub bucket_name: String,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
#[schemars(example = "Self::example")]
pub struct S3Storage {
    pub details: S3Details,

    pub tables: Vec<Table>,
}

impl S3Storage {
    pub fn convert_to_table(&self) -> PrettyTable {
        table!(
            ["access_key_id", SECRET],
            ["secret_access_key", SECRET],
            ["region", self.details.region],
            ["bucket_name", self.details.bucket_name]
        )
    }
}
impl SchemaExample for S3Storage {
    fn example() -> Self {
        let s3_details = S3Details {
            access_key_id: "".to_owned(),
            secret_access_key: "".to_owned(),
            region: "".to_owned(),
            bucket_name: "".to_owned(),
        };
        Self {
            details: s3_details,
            tables: vec![Table {
                config: TableConfig::CSV(CsvConfig {
                    path: "path/to/file".to_owned(),
                    extension: ".csv".to_owned(),
                    marker_extension: None,
                }),
                name: "table_name".to_owned(),
            }],
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub struct LocalDetails {
    pub path: String,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
#[schemars(example = "Self::example")]

pub struct LocalStorage {
    pub details: LocalDetails,

    pub tables: Vec<Table>,
}

impl LocalStorage {
    pub fn convert_to_table(&self) -> PrettyTable {
        table!(["path", self.details.path])
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
#[schemars(example = "Self::example")]

pub struct DeltaLakeConfig {
    pub tables: Vec<DeltaTable>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
#[schemars(example = "Self::example")]

pub struct MongodbConfig {
    pub connection_string: String,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
#[schemars(example = "Self::example")]

pub struct MySQLConfig {
    pub url: String,

    pub server_id: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub struct NestedDozerConfig {
    pub url: String,
    #[serde(default, skip_serializing_if = "equal_default")]
    pub log_options: NestedDozerLogOptions,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema, Default)]
pub struct NestedDozerLogOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_in_millis: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub buffer_size: Option<u32>,
}

pub fn default_log_batch_size() -> u32 {
    30
}

pub fn default_timeout() -> u32 {
    1000
}

pub fn default_buffer_size() -> u32 {
    1000
}

pub fn default_snowflake_poll_interval() -> Duration {
    Duration::from_secs(60)
}

impl SchemaExample for MongodbConfig {
    fn example() -> Self {
        Self {
            connection_string: "mongodb://localhost:27017/db_name".to_owned(),
        }
    }
}

impl SchemaExample for MySQLConfig {
    fn example() -> Self {
        Self {
            url: "mysql://root:1234@localhost:3306/db_name".to_owned(),
            server_id: Some((1).to_owned()),
        }
    }
}

impl SchemaExample for GrpcConfig {
    fn example() -> Self {
        Self {
            host: Some("localhost".to_owned()),
            port: Some(50051),
            schemas: ConfigSchemas::Path("schema.json".to_owned()),
            adapter: Some("arrow".to_owned()),
        }
    }
}

impl SchemaExample for KafkaConfig {
    fn example() -> Self {
        Self {
            broker: "".to_owned(),
            schema_registry_url: Some("".to_owned()),
        }
    }
}

impl SchemaExample for DeltaLakeConfig {
    fn example() -> Self {
        Self {
            tables: vec![DeltaTable {
                path: "".to_owned(),
                name: "".to_owned(),
            }],
        }
    }
}

impl SchemaExample for LocalStorage {
    fn example() -> Self {
        Self {
            details: LocalDetails {
                path: "path".to_owned(),
            },
            tables: vec![Table {
                config: TableConfig::CSV(CsvConfig {
                    path: "path/to/table".to_owned(),
                    extension: ".csv".to_owned(),
                    marker_extension: None,
                }),
                name: "table_name".to_owned(),
            }],
        }
    }
}

impl SchemaExample for SnowflakeConfig {
    fn example() -> Self {
        Self {
            server: "<account_name>.<region_id>.snowflakecomputing.com".to_owned(),
            port: "443".to_owned(),
            user: "bob".to_owned(),
            password: "password".to_owned(),
            database: "database".to_owned(),
            schema: "schema".to_owned(),
            warehouse: "warehouse".to_owned(),
            driver: Some("SnowflakeDSIIDriver".to_owned()),
            role: "role".to_owned(),
            poll_interval_seconds: None,
        }
    }
}

impl SchemaExample for EthConfig {
    fn example() -> Self {
        let eth_filter = EthFilter {
            from_block: Some(0),
            to_block: None,
            addresses: vec![],
            topics: vec![],
        };
        Self {
            provider: EthProviderConfig::Log(EthLogConfig {
                wss_url: "".to_owned(),
                filter: Some(eth_filter),
                contracts: vec![],
            }),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, Default, JsonSchema)]
pub struct JavaScriptConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bootstrap_path: Option<String>,
}

pub fn default_bootstrap_path() -> String {
    String::from("src/js/bootstrap.js")
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
#[schemars(example = "Self::example")]
pub struct WebhookConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u32>,

    pub endpoints: Vec<WebhookEndpoint>,
}
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
#[schemars(example = "Self::example")]
pub struct WebhookEndpoint {
    pub path: String,
    pub verbs: Vec<WebhookVerb>,
    pub schema: WebhookConfigSchemas,
}
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
#[schemars(example = "Self::example")]
pub enum WebhookVerb {
    POST,   // insert
    PUT,    // update
    DELETE, // delete
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub enum WebhookConfigSchemas {
    Inline(String),
    Path(String),
}

impl SchemaExample for WebhookConfig {
    fn example() -> Self {
        Self {
            host: Some("localhost".to_owned()),
            port: Some(50059),
            endpoints: vec![WebhookEndpoint::example()],
        }
    }
}

impl SchemaExample for WebhookEndpoint {
    fn example() -> Self {
        let user_schema = r#"
        {
            "users": {
              "schema": {
                "fields": [
                  {
                    "name": "id",
                    "typ": "Int",
                    "nullable": false
                  },
                  {
                    "name": "name",
                    "typ": "String",
                    "nullable": true
                  },
                  {
                    "name": "json",
                    "typ": "Json",
                    "nullable": true
                  }
                ]
              }
            }
          }
        "#;
        Self {
            path: "/ingest".to_owned(),
            verbs: vec![WebhookVerb::POST, WebhookVerb::DELETE],
            schema: WebhookConfigSchemas::Inline(user_schema.to_string()),
        }
    }
}
impl SchemaExample for WebhookVerb {
    fn example() -> Self {
        Self::POST
    }
}

#[derive(Debug, JsonSchema, Clone, Deserialize, Serialize, Hash, Eq, PartialEq)]
pub struct AerospikeConfig {
    pub namespace: String,
    pub sets: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash, JsonSchema)]
pub struct OracleConfig {
    pub user: String,
    pub password: String,
    pub host: String,
    pub port: u16,
    pub sid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Only needed if using pluggable database
    pub pdb: Option<String>,
    /// The schemas to consider when listing tables. If empty, will list all schemas, which can be slow.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub schemas: Vec<String>,
    /// A `VARRAY OF VARCHAR2` type that can be used by dozer.
    pub string_collection_type_name: String,
    /// Batch size during snapshotting
    pub batch_size: Option<usize>,
    pub replicator: OracleReplicator,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Copy, Hash, JsonSchema)]
pub enum OracleReplicator {
    LogMiner { poll_interval_in_milliseconds: u64 },
    DozerLogReader,
}
