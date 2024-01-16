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
    pub kind: EndpointKind,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub enum EndpointKind {
    Api(ApiEndpoint),
    Dummy,
    Aerospike(AerospikeSinkConfig),
    BigQuery(BigQuery),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone)]
pub struct AerospikeSinkConfig {
    pub namespace: String,
    pub hosts: String,
    #[serde(default)]
    pub n_threads: Option<NonZeroUsize>,
    #[serde(default)]
    pub set_name: String,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BigQuery {
    pub auth: bigquery::Authentication,
    pub destination: bigquery::Destination,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options: Option<bigquery::Options>,
}

pub mod bigquery {
    use crate::helper::{deserialize_decimal_as_f64, f64_opt_schema, serialize_decimal_as_f64};
    use rust_decimal::Decimal;
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
    #[serde(deny_unknown_fields)]
    pub struct Authentication {
        pub service_account_key: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
    #[serde(deny_unknown_fields)]
    pub struct Destination {
        pub stage_gcs_bucket_name: String,
        pub project: String,
        pub dataset: String,
        pub dataset_location: String,
        pub table: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
    #[serde(deny_unknown_fields)]
    pub struct Options {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub batch_size: Option<usize>,

        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "deserialize_decimal_as_f64",
            serialize_with = "serialize_decimal_as_f64"
        )]
        #[schemars(schema_with = "f64_opt_schema")]
        pub stage_max_size_in_mb: Option<Decimal>,
    }

    impl Default for Options {
        fn default() -> Self {
            Self {
                batch_size: Some(default_batch_size()),
                stage_max_size_in_mb: Some(default_stage_max_size_in_mb()),
            }
        }
    }

    pub fn default_batch_size() -> usize {
        1000000
    }

    pub fn default_stage_max_size_in_mb() -> Decimal {
        Decimal::from(215)
    }
}
