use std::path::Path;

use super::{
    api_config::ApiConfig, api_endpoint::ApiEndpoint, app_config::AppConfig, cloud::Cloud,
    connection::Connection, flags::Flags, source::Source, telemetry::TelemetryConfig,
};
use crate::constants::DEFAULT_HOME_DIR;
use crate::models::udf_config::UdfConfig;
use prettytable::Table as PrettyTable;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, prost::Message)]
/// The configuration for the app
pub struct Config {
    #[prost(string, tag = "2")]
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

    #[prost(message, optional, tag = "8")]
    /// Api server config related: port, host, etc
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api: Option<ApiConfig>,

    #[prost(string, optional, tag = "9")]
    #[serde(skip_serializing_if = "Option::is_none")]
    /// transformations to apply to source data in SQL format as multiple queries
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[prost(message, optional, tag = "10")]
    /// flags to enable/disable features
    pub flags: Option<Flags>,

    /// Cache lmdb max map size
    #[prost(uint64, optional, tag = "11")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_max_map_size: Option<u64>,

    #[prost(message, optional, tag = "12")]
    /// App runtime config: behaviour of pipeline and log
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app: Option<AppConfig>,

    #[prost(message, optional, tag = "13")]
    /// Instrument using Dozer
    #[serde(skip_serializing_if = "Option::is_none")]
    pub telemetry: Option<TelemetryConfig>,
    #[prost(message, optional, tag = "14")]
    /// Dozer Cloud specific configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cloud: Option<Cloud>,

    #[prost(message, repeated, tag = "15")]
    /// UDF specific configuration (eg. !Onnx)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub udfs: Vec<UdfConfig>,
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

pub fn default_cache_max_map_size() -> u64 {
    1024 * 1024 * 1024
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
