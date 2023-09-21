use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone, PartialEq, Eq, Default)]
pub struct AppConfig {
    /// Pipeline buffer size

    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_buffer_size: Option<u32>,

    /// Commit size

    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_size: Option<u32>,

    /// Commit timeout

    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_timeout: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub persist_queue_capacity: Option<u32>,

    /// The storage to use for the log.

    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_storage: Option<DataStorage>,

    /// How many errors we can tolerate before bringing down the app.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_threshold: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    /// The maximum unpersisted number of records in the processor record store. A checkpoint will be created when this number is reached.
    pub max_num_records_before_persist: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    /// The maximum time in seconds before a new checkpoint is created. If there're no new records, no checkpoint will be created.
    pub max_interval_before_persist_in_seconds: Option<u64>,
}

#[derive(Debug, JsonSchema, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataStorage {
    Local(()),

    S3(S3Storage),
}

#[derive(Debug, JsonSchema, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct S3Storage {
    pub region: String,

    pub bucket_name: String,
}

impl Default for DataStorage {
    fn default() -> Self {
        Self::Local(())
    }
}

pub fn default_persist_queue_capacity() -> u32 {
    100
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

pub fn default_error_threshold() -> u32 {
    0
}

pub fn default_max_num_records_before_persist() -> u64 {
    100_000
}

pub fn default_max_interval_before_persist_in_seconds() -> u64 {
    60
}
