use std::num::NonZeroUsize;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::equal_default;

#[derive(Debug, Serialize, Deserialize, JsonSchema, Default, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct ApiIndex {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub primary_key: Vec<String>,

    #[serde(default, skip_serializing_if = "equal_default")]
    pub secondary: SecondaryIndexConfig,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Default, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct SecondaryIndexConfig {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skip_default: Vec<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub create: Vec<SecondaryIndex>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub enum SecondaryIndex {
    SortedInverted(SortedInverted),
    FullText(FullText),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct SortedInverted {
    pub fields: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct FullText {
    pub field: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone, Copy, Default)]
#[serde(deny_unknown_fields)]
pub enum OnInsertResolutionTypes {
    #[default]
    Nothing,
    Update,
    Panic,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone, Copy, Default)]
#[serde(deny_unknown_fields)]
pub enum OnUpdateResolutionTypes {
    #[default]
    Nothing,
    Upsert,
    Panic,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone, Copy, Default)]
#[serde(deny_unknown_fields)]
pub enum OnDeleteResolutionTypes {
    #[default]
    Nothing,
    Panic,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Default, Eq, PartialEq, Clone, Copy)]
#[serde(deny_unknown_fields)]
pub struct ConflictResolution {
    #[serde(default, skip_serializing_if = "equal_default")]
    pub on_insert: OnInsertResolutionTypes,
    #[serde(default, skip_serializing_if = "equal_default")]
    pub on_update: OnUpdateResolutionTypes,
    #[serde(default, skip_serializing_if = "equal_default")]
    pub on_delete: OnDeleteResolutionTypes,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Default, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct LogReaderOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_in_millis: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub buffer_size: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct Sink {
    pub name: String,
    pub config: SinkConfig,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
#[allow(clippy::large_enum_variant)]
pub enum SinkConfig {
    Dummy(DummySinkConfig),
    Aerospike(AerospikeSinkConfig),
    Clickhouse(ClickhouseSinkConfig),
    Oracle(OracleSinkConfig),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DummySinkConfig {
    pub table_name: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum DenormColumn {
    Direct(String),
    Renamed { source: String, destination: String },
}

impl DenormColumn {
    pub fn to_src_dst(&self) -> (&str, &str) {
        match self {
            DenormColumn::Direct(name) => (name, name),
            DenormColumn::Renamed {
                source,
                destination,
            } => (source, destination),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum DenormKey {
    Simple(String),
    Composite(Vec<String>),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AerospikeDenormalizations {
    pub from_namespace: String,
    pub from_set: String,
    pub key: DenormKey,
    pub columns: Vec<DenormColumn>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone, PartialEq, Eq)]
pub struct AerospikeSet {
    pub namespace: String,
    pub set: String,
    pub primary_key: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AerospikeSinkTable {
    pub source_table_name: String,
    pub namespace: String,
    pub set_name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub denormalize: Vec<AerospikeDenormalizations>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_denormalized_to: Option<AerospikeSet>,
    #[serde(default)]
    pub primary_key: Vec<String>,
    #[serde(default)]
    pub aggregate_by_pk: bool,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct AerospikeSinkConfig {
    pub connection: String,
    pub n_threads: Option<NonZeroUsize>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<AerospikeSinkTable>,
    pub max_batch_duration_ms: Option<u64>,
    pub preferred_batch_size: Option<u64>,
    pub metadata_namespace: String,
    #[serde(default)]
    pub metadata_set: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct ClickhouseSinkConfig {
    #[serde(default = "ClickhouseSinkConfig::default_host")]
    pub host: String,
    #[serde(default = "ClickhouseSinkConfig::default_port")]
    pub port: u16,
    #[serde(default = "ClickhouseSinkConfig::default_user")]
    pub user: String,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default = "ClickhouseSinkConfig::default_scheme")]
    pub scheme: String,
    #[serde(default = "ClickhouseSinkConfig::default_database")]
    pub database: String,
    pub options: Vec<(String, String)>,
    pub source_table_name: String,
    pub sink_table_name: String,
    pub create_table_options: Option<ClickhouseTableOptions>,
}

impl ClickhouseSinkConfig {
    fn default_database() -> String {
        "default".to_string()
    }
    fn default_scheme() -> String {
        "tcp".to_string()
    }
    fn default_host() -> String {
        "0.0.0.0".to_string()
    }
    fn default_port() -> u16 {
        9000
    }
    fn default_user() -> String {
        "default".to_string()
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct ClickhouseTableOptions {
    pub engine: Option<String>,
    pub primary_keys: Option<Vec<String>>,
    pub partition_by: Option<String>,
    pub sample_by: Option<String>,
    pub order_by: Option<Vec<String>>,
    pub cluster: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct OracleSinkConfig {
    pub connection: String,
    pub table_name: String,
    #[serde(default)]
    pub unique_key: Vec<String>,
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(default)]
    pub append_only: bool,
}

pub fn default_log_reader_batch_size() -> u32 {
    1000
}

pub fn default_log_reader_timeout_in_millis() -> u32 {
    300
}

pub fn default_log_reader_buffer_size() -> u32 {
    1000
}

impl std::fmt::Display for SecondaryIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SecondaryIndex::SortedInverted(SortedInverted { fields }) => {
                write!(f, "type: SortedInverted, fields: {}", fields.join(", "))
            }
            SecondaryIndex::FullText(FullText { field }) => {
                write!(f, "type: FullText, field: {}", field)
            }
        }
    }
}
