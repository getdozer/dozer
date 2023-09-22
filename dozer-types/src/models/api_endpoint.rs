use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, JsonSchema, Default, Deserialize, Eq, PartialEq, Clone)]
pub struct ApiIndex {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub primary_key: Vec<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secondary: Option<SecondaryIndexConfig>,
}

#[derive(Debug, Serialize, JsonSchema, Default, Deserialize, Eq, PartialEq, Clone)]
pub struct SecondaryIndexConfig {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skip_default: Vec<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub create: Vec<CreateSecondaryIndex>,
}

#[derive(Debug, Serialize, JsonSchema, Default, Deserialize, Eq, PartialEq, Clone)]
pub struct CreateSecondaryIndex {
    pub index: Option<SecondaryIndex>,
}

#[derive(Debug, Serialize, JsonSchema, Deserialize, Eq, PartialEq, Clone)]
pub enum SecondaryIndex {
    SortedInverted(SortedInverted),

    FullText(FullText),
}

#[derive(Debug, Serialize, JsonSchema, Default, Deserialize, Eq, PartialEq, Clone)]
pub struct SortedInverted {
    pub fields: Vec<String>,
}

#[derive(Debug, Serialize, JsonSchema, Default, Deserialize, Eq, PartialEq, Clone)]
pub struct FullText {
    pub field: String,
}

#[derive(Debug, Serialize, JsonSchema, Deserialize, Eq, PartialEq, Clone, Copy)]
pub enum OnInsertResolutionTypes {
    Nothing(()),

    Update(()),

    Panic(()),
}

impl Default for OnInsertResolutionTypes {
    fn default() -> Self {
        OnInsertResolutionTypes::Nothing(())
    }
}

#[derive(Debug, Serialize, JsonSchema, Deserialize, Eq, PartialEq, Clone, Copy)]
pub enum OnUpdateResolutionTypes {
    Nothing(()),

    Upsert(()),

    Panic(()),
}

impl Default for OnUpdateResolutionTypes {
    fn default() -> Self {
        OnUpdateResolutionTypes::Nothing(())
    }
}

#[derive(Debug, Serialize, JsonSchema, Deserialize, Eq, PartialEq, Clone, Copy)]
pub enum OnDeleteResolutionTypes {
    Nothing(()),

    Panic(()),
}

impl Default for OnDeleteResolutionTypes {
    fn default() -> Self {
        OnDeleteResolutionTypes::Nothing(())
    }
}

#[derive(Debug, Serialize, JsonSchema, Default, Deserialize, Eq, PartialEq, Clone, Copy)]
pub struct ConflictResolution {
    pub on_insert: Option<OnInsertResolutionTypes>,

    pub on_update: Option<OnUpdateResolutionTypes>,

    pub on_delete: Option<OnDeleteResolutionTypes>,
}

#[derive(Debug, Serialize, JsonSchema, Default, Deserialize, Eq, PartialEq, Clone)]
pub struct LogReaderOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_in_millis: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub buffer_size: Option<u32>,
}

#[derive(Debug, Serialize, JsonSchema, Default, Deserialize, Eq, PartialEq, Clone)]
pub struct ApiEndpoint {
    pub name: String,

    /// name of the table in source database; Type: String
    pub table_name: String,

    /// path of endpoint - e.g: /stocks
    pub path: String,

    pub index: Option<ApiIndex>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub conflict_resolution: Option<ConflictResolution>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_reader_options: Option<LogReaderOptions>,
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
