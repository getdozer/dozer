use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, prost::Message)]
pub struct AppConfig {
    /// Pipeline buffer size
    #[prost(uint32, optional)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_buffer_size: Option<u32>,

    /// Commit size
    #[prost(uint32, optional)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_size: Option<u32>,

    /// Commit timeout
    #[prost(uint64, optional)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_timeout: Option<u64>,

    /// Max number of operations in one log entry.
    #[prost(uint64, optional)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_entry_max_size: Option<u64>,

    /// The storage to use for the log.
    #[prost(oneof = "LogStorage", tags = "7,8")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_storage: Option<LogStorage>,

    #[prost(uint32, optional)]
    /// How many errors we can tolerate before bringing down the app.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub err_threshold: Option<u32>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, prost::Oneof)]
pub enum LogStorage {
    #[prost(message, tag = "1")]
    Local(()),
    #[prost(string, tag = "2")]
    S3(String),
}

impl Default for LogStorage {
    fn default() -> Self {
        Self::Local(())
    }
}

pub fn default_app_buffer_size() -> u32 {
    20_000
}

pub fn default_commit_size() -> u32 {
    10_000
}

pub fn default_commit_timeout() -> u64 {
    50
}

pub fn default_log_entry_max_size() -> u64 {
    100_000
}

pub fn default_err_threshold() -> u32 {
    0
}
