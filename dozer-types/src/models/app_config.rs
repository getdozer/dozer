use std::path::Path;

use super::{
    api_config::ApiConfig, api_endpoint::ApiEndpoint, cloud::Cloud, connection::Connection,
    flags::Flags, source::Source, telemetry::TelemetryConfig,
};
use crate::constants::DEFAULT_HOME_DIR;
use prettytable::Table as PrettyTable;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, prost::Message)]
/// The configuration for the app
pub struct Config {
    #[prost(string, tag = "2")]
    #[serde(skip_serializing_if = "String::is_empty")]
    /// name of the app
    pub app_name: String,

    #[prost(string, tag = "3")]
    #[serde(skip_serializing_if = "String::is_empty", default = "default_home_dir")]
    ///directory for all process; Default: ./.dozer
    pub home_dir: String,

    #[prost(string, tag = "4")]
    #[serde(
        skip_serializing_if = "String::is_empty",
        default = "default_cache_dir"
    )]
    ///directory for cache. Default: ./.dozer/cache
    pub cache_dir: String,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[prost(message, repeated, tag = "5")]
    /// connections to databases: Eg: Postgres, Snowflake, etc
    pub connections: Vec<Connection>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[prost(message, repeated, tag = "6")]
    /// sources to ingest data related to particular connection
    pub sources: Vec<Source>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[prost(message, repeated, tag = "7")]
    /// api endpoints to expose
    pub endpoints: Vec<ApiEndpoint>,

    #[prost(message, tag = "8")]
    /// Api server config related: port, host, etc
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api: Option<ApiConfig>,

    #[prost(string, optional, tag = "9")]
    #[serde(skip_serializing_if = "Option::is_none")]
    /// transformations to apply to source data in SQL format as multiple queries
    pub sql: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[prost(message, tag = "10")]
    /// flags to enable/disable features
    pub flags: Option<Flags>,

    /// Cache lmdb max map size
    #[prost(uint64, optional, tag = "11")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_max_map_size: Option<u64>,

    /// Pipeline buffer size
    #[prost(uint32, optional, tag = "12")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_buffer_size: Option<u32>,

    /// Commit size
    #[prost(uint32, optional, tag = "13")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_size: Option<u32>,

    /// Commit timeout
    #[prost(uint64, optional, tag = "14")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_timeout: Option<u64>,

    /// Max number of operations in one log entry.
    #[prost(uint64, optional, tag = "15")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_entry_max_size: Option<u64>,

    #[prost(message, tag = "16")]
    /// Instrument using Dozer
    #[serde(skip_serializing_if = "Option::is_none")]
    pub telemetry: Option<TelemetryConfig>,

    #[prost(message, tag = "17")]
    /// Dozer Cloud specific configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cloud: Option<Cloud>,

    #[prost(message, tag = "18")]
    /// Dozer Cloud specific configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub err_threshold: Option<u32>,
}

pub fn default_home_dir() -> String {
    DEFAULT_HOME_DIR.to_owned()
}

pub fn get_cache_dir(home_dir: &str) -> String {
    AsRef::<Path>::as_ref(home_dir)
        .join("cache")
        .to_str()
        .unwrap()
        .to_string()
}
pub fn default_cache_dir() -> String {
    get_cache_dir(DEFAULT_HOME_DIR)
}

pub fn default_log_entry_max_size() -> u64 {
    100_000
}

pub fn default_cache_max_map_size() -> u64 {
    1024 * 1024 * 1024
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

pub fn default_err_threshold() -> u32 {
    0
}

impl Config {
    pub fn convert_to_table(&self) -> PrettyTable {
        let mut table = table!();

        table.add_row(row!["name", self.app_name]);
        table.add_row(row![
            "connectors",
            self.connections
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<String>>()
                .join(", ")
        ]);
        let mut endpoints_table = table!();
        for endpoint in &self.endpoints {
            endpoints_table.add_row(row![endpoint.name, endpoint.table_name, endpoint.path]);
        }
        if !self.endpoints.is_empty() {
            table.add_row(row!["endpoints", endpoints_table]);
        }

        table
    }
}
