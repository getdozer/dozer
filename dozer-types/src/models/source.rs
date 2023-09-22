use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, JsonSchema, Default, Deserialize, Eq, PartialEq, Clone)]
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
    /// name of schema source database; Type: String

    #[serde(default)]
    pub schema: Option<String>,

    #[serde(default = "default_refresh_config")]
    #[serde(skip_serializing_if = "Option::is_none")]
    /// setting for how to refresh the data; Default: RealTime
    pub refresh_config: Option<RefreshConfig>,
}

fn default_refresh_config() -> Option<RefreshConfig> {
    Some(RefreshConfig::default())
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum Value {
    Ref(String),
}

#[derive(Debug, Serialize, JsonSchema, Deserialize, Eq, PartialEq, Clone)]
pub enum HistoryType {
    Master(MasterHistoryConfig),
    Transactional(TransactionalHistoryConfig),
}
#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone)]
pub enum MasterHistoryConfig {
    AppendOnly {
        unique_key_field: String,
        open_date_field: String,
        closed_date_field: String,
    },
    Overwrite,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq, Clone)]
pub enum TransactionalHistoryConfig {
    RetainPartial {
        timestamp_field: String,
        retention_period: u32,
    },
}
#[derive(Debug, Serialize, JsonSchema, Deserialize, Eq, PartialEq, Clone)]
pub enum RefreshConfig {
    // Hour { minute: u32 },
    // Day { time: String },
    // CronExpression { expression: String },
    RealTime(RealTimeConfig),
}
impl Default for RefreshConfig {
    fn default() -> Self {
        RefreshConfig::RealTime(RealTimeConfig {})
    }
}
#[derive(Debug, Serialize, JsonSchema, Default, Deserialize, Eq, PartialEq, Clone)]
pub struct RealTimeConfig {}
