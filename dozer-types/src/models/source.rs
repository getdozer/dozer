use super::connection::Connection;
use serde::{ser::SerializeStruct, Deserialize, Serialize};

#[derive(Deserialize, Eq, PartialEq, Clone, ::prost::Message)]
pub struct Source {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[prost(string, optional, tag = "1")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[prost(string, optional, tag = "2")]
    pub app_id: Option<String>,
    #[prost(string, tag = "3")]
    pub name: String,
    #[prost(string, tag = "4")]
    pub table_name: String,
    #[prost(string, repeated, tag = "5")]
    pub columns: Vec<String>,
    #[prost(message, tag = "6")]
    #[serde(skip_deserializing)]
    pub connection: Option<Connection>,
    #[prost(oneof="RefreshConfig", tags = "7")]
    pub refresh_config: Option<RefreshConfig>,
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
        state.serialize_field("connection", &Value::Ref(self.connection.to_owned().unwrap_or_default().name))?;
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
    #[prost(message, tag = "1")]
    RealTime(RealTimeConfig),
}
impl Default for RefreshConfig {
    fn default() -> Self {
        RefreshConfig::RealTime(RealTimeConfig {})
    }
}
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message)]
pub struct RealTimeConfig {}
