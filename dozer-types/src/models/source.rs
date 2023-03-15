use super::connection::Connection;
use serde::{ser::SerializeStruct, Deserialize, Serialize};

#[derive(Deserialize, Eq, PartialEq, Clone, ::prost::Message)]
pub struct Source {
    #[prost(string, tag = "1")]
    /// name of the source - to distinguish between multiple sources; Type: String
    pub name: String,
    #[prost(string, tag = "2")]
    /// name of the table in source database; Type: String
    pub table_name: String,
    #[prost(string, repeated, tag = "3")]
    /// list of columns gonna be used in the source table; Type: String[]
    pub columns: Vec<String>,
    #[prost(message, tag = "4")]
    #[serde(skip_deserializing)]
    /// reference to pre-defined connection name - syntax: `!Ref <connection_name>`; Type: `Ref!` tag
    pub connection: Option<Connection>,
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
impl Serialize for Source {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("Source", 5)?;
        state.serialize_field("name", &self.name)?;
        state.serialize_field("table_name", &self.table_name)?;
        state.serialize_field("columns", &self.columns)?;
        state.serialize_field("schema", &self.schema)?;
        state.serialize_field(
            "connection",
            &Value::Ref(self.connection.to_owned().unwrap_or_default().name),
        )?;
        state.serialize_field("refresh_config", &self.refresh_config)?;
        state.end()
    }
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
