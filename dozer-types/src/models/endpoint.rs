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
    pub kind: EndpointKind,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub enum EndpointKind {
    Api(ApiEndpoint),
    Dummy,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct ApiEndpoint {
    pub name: String,

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
