use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::equal_default;

#[derive(Debug, Serialize, Deserialize, JsonSchema, Default, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct Source {
    /// name of the source - to distinguish between multiple sources; Type: String
    pub name: String,

    /// name of the table in source database; Type: String
    pub table_name: String,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    /// list of columns gonna be used in the source table; Type: String[]
    pub columns: Vec<String>,

    /// reference to pre-defined connection name; Type: String
    pub connection: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    /// name of schema source database; Type: String
    pub schema: Option<String>,

    #[serde(default, skip_serializing_if = "equal_default")]
    /// setting for how to refresh the data; Default: RealTime
    pub refresh_config: RefreshConfig,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone, Default)]
#[serde(deny_unknown_fields)]
pub enum RefreshConfig {
    // Hour { minute: u32 },
    // Day { time: String },
    // CronExpression { expression: String },
    #[default]
    RealTime,
}
