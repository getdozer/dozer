use super::connection::Connection;
use serde::{ser::SerializeStruct, Deserialize, Serialize};

#[derive(Debug, Deserialize, Eq, PartialEq, Clone)]
pub struct Source {
    pub id: Option<String>,
    pub name: String,
    pub table_name: String,
    pub columns: Option<Vec<String>>,
    #[serde(skip_deserializing)]
    pub connection: Connection,
    pub history_type: Option<HistoryType>,
    pub refresh_config: RefreshConfig,
}
impl Serialize for Source {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("Source", 6)?;
        state.serialize_field("name", &self.name)?;
        state.serialize_field("table_name", &self.table_name)?;
        state.serialize_field("columns", &self.columns)?;
        state.serialize_field("connection", &Value::Ref(self.connection.name.to_owned()))?;
        state.serialize_field("history_type", &self.history_type)?;
        state.serialize_field("refresh_config", &self.refresh_config)?;
        state.end()
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum Value {
    Ref(String),
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
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
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum RefreshConfig {
    Hour { minute: u32 },
    Day { time: String },
    CronExpression { expression: String },
    RealTime,
}
