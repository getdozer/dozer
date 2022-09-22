use super::connection::Connection;
use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Source {
    pub id: Option<String>,
    pub name: String,
    pub dest_table_name: String,
    pub connection: Connection,
    pub history_type: HistoryType,
    pub refresh_config: RefreshConfig
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum HistoryType {
    Master(MasterHistoryConfig),
    Transactional(TransactionalHistoryConfig),
}
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum MasterHistoryConfig {
    AppendOnly {
        unique_key_field: String,
        open_date_field: String,
        closed_date_field: String,
    },
    Overwrite,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum TransactionalHistoryConfig {
    RetainPartial {
        timestamp_field: String,
        retention_period: u32,
    },
}
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum RefreshConfig {
  Hour {minute: u32},
  Day {time: String},
  CronExpression {expression: String},
  RealTime
}

