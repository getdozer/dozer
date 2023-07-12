use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message)]
pub struct Source {
    #[prost(string, tag = "1")]
    /// name of the source - to distinguish between multiple sources; Type: String
    pub name: String,
    #[prost(string, tag = "2")]
    /// name of the table in source database; Type: String
    pub table_name: String,
    #[prost(string, repeated, tag = "3")]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    /// list of columns gonna be used in the source table; Type: String[]
    pub columns: Vec<String>,
    #[prost(string, tag = "4")]
    /// reference to pre-defined connection name; Type: String
    pub connection: String,
    /// name of schema source database; Type: String
    #[prost(string, optional, tag = "5")]
    #[serde(default)]
    pub schema: Option<String>,
    #[prost(oneof = "RefreshConfig", tags = "7")]
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

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug)]
pub enum HistoryType {
    Master(MasterHistoryConfig),
    Transactional(TransactionalHistoryConfig),
}
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum MasterHistoryConfig {
    AppendOnly {
        unique_key_field: String,
        open_date_field: String,
        closed_date_field: String,
    },
    Overwrite,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum TransactionalHistoryConfig {
    RetainPartial {
        timestamp_field: String,
        retention_period: u32,
    },
}
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Oneof)]
pub enum RefreshConfig {
    // Hour { minute: u32 },
    // Day { time: String },
    // CronExpression { expression: String },
    #[prost(message, tag = "7")]
    RealTime(RealTimeConfig),
}
impl Default for RefreshConfig {
    fn default() -> Self {
        RefreshConfig::RealTime(RealTimeConfig {})
    }
}
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message)]
pub struct RealTimeConfig {}
