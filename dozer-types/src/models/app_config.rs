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

    #[prost(uint32, optional)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persist_queue_capacity: Option<u32>,

    /// The storage to use for the log.
    #[prost(oneof = "DataStorage", tags = "7,8")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_storage: Option<DataStorage>,

    #[prost(uint32, optional)]
    /// How many errors we can tolerate before bringing down the app.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_threshold: Option<u32>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, prost::Oneof)]
pub enum DataStorage {
    #[prost(message, tag = "7")]
    Local(()),
    #[prost(message, tag = "8")]
    S3(S3Storage),
}

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize, prost::Message)]
pub struct S3Storage {
    #[prost(string, tag = "1")]
    pub region: String,
    #[prost(string, tag = "2")]
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
