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
pub struct Endpoint {
    /// name of the table in source database; Type: String
    pub table_name: String,

    /// endpoint kind
    pub config: EndpointKind,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub enum EndpointKind {
    Api(ApiEndpoint),
    Dummy,
    Aerospike(AerospikeSinkConfig),
    Clickhouse(ClickhouseSinkConfig),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone, PartialEq, Eq)]
pub struct AerospikeDenormalizations {
    pub from_namespace: String,
    pub from_set: String,
    pub key: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone)]
pub struct AerospikeSinkConfig {
    pub connection: String,
    #[serde(default)]
    pub n_threads: Option<NonZeroUsize>,
    #[serde(default)]
    pub denormalize: Vec<AerospikeDenormalizations>,
    pub namespace: String,
    pub set_name: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone)]
pub struct ClickhouseSinkConfig {
    pub database_url: String,
    pub user: String,
    #[serde(default)]
    pub password: Option<String>,
    pub database: String,
    pub sink_table_name: String,
    pub primary_keys: Option<Vec<String>>,
    pub create_table_options: Option<ClickhouseSinkTableOptions>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone)]
pub struct ClickhouseSinkTableOptions {
    pub engine: Option<String>,
    pub partition_by: Option<String>,
    pub sample_by: Option<String>,
    pub order_by: Option<Vec<String>>,
    pub cluster: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct ApiEndpoint {
    /// path of endpoint - e.g: /stocks
    pub path: String,

    #[serde(default, skip_serializing_if = "equal_default")]
    pub index: ApiIndex,

    #[serde(default, skip_serializing_if = "equal_default")]
    pub conflict_resolution: ConflictResolution,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,

    #[serde(default, skip_serializing_if = "equal_default")]
    pub log_reader_options: LogReaderOptions,
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
