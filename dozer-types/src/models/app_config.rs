use std::path::Path;

use super::{
    api_config::ApiConfig, api_endpoint::ApiEndpoint, cloud::Cloud, connection::Connection,
    flags::Flags, source::Source, telemetry::TelemetryConfig,
};
use crate::{constants::DEFAULT_HOME_DIR, models::api_config::default_api_config};
use prettytable::Table as PrettyTable;
use serde::{
    de::{self, IgnoredAny, Visitor},
    Deserialize, Deserializer, Serialize,
};

#[derive(Serialize, PartialEq, Eq, Clone, prost::Message)]
/// The configuration for the app
pub struct Config {
    #[prost(string, tag = "2")]
    /// name of the app
    pub app_name: String,

    #[prost(string, tag = "3")]
    #[serde(default = "default_home_dir")]
    ///directory for all process; Default: ./.dozer
    pub home_dir: String,

    #[prost(string, tag = "4")]
    #[serde(default = "default_cache_dir")]
    ///directory for cache. Default: ./.dozer/cache
    pub cache_dir: String,

    #[prost(message, repeated, tag = "5")]
    /// connections to databases: Eg: Postgres, Snowflake, etc
    pub connections: Vec<Connection>,

    #[prost(message, repeated, tag = "6")]
    /// sources to ingest data related to particular connection
    pub sources: Vec<Source>,

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

    /// Buffer capacity for Log Writer
    #[prost(uint64, optional, tag = "15")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_buffer_capacity: Option<u64>,

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

pub fn default_file_buffer_capacity() -> u64 {
    1024 * 1024 * 1024
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

impl<'de> Deserialize<'de> for Config {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ConfigVisitor;

        impl<'de> Visitor<'de> for ConfigVisitor {
            type Value = Config;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("Dozer Config")
            }

            fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut api: Option<ApiConfig> = Some(default_api_config());
                let mut flags: Option<Flags> = Some(Flags::default());
                let mut connections: Vec<Connection> = vec![];
                let mut sources_value: Vec<serde_yaml::Value> = vec![];
                let mut endpoints_value: Vec<serde_yaml::Value> = vec![];
                let mut telemetry: Option<TelemetryConfig> = None;
                let mut cloud: Option<Cloud> = None;

                let mut app_name = "".to_owned();
                let mut sql = None;
                let mut home_dir: String = default_home_dir();
                let mut cache_dir: String = default_cache_dir();

                let mut file_buffer_capacity: Option<u64> = Some(default_file_buffer_capacity());
                let mut cache_max_map_size: Option<u64> = Some(default_cache_max_map_size());
                let mut app_buffer_size: Option<u32> = Some(default_app_buffer_size());
                let mut commit_size: Option<u32> = Some(default_commit_size());
                let mut commit_timeout: Option<u64> = Some(default_commit_timeout());
                let mut err_threshold: Option<u32> = Some(default_err_threshold());

                while let Some(key) = access.next_key()? {
                    match key {
                        "app_name" => {
                            app_name = access.next_value::<String>()?;
                        }
                        "api" => {
                            api = Some(access.next_value::<ApiConfig>()?);
                        }
                        "flags" => {
                            flags = Some(access.next_value::<Flags>()?);
                        }
                        "connections" => {
                            connections = access.next_value::<Vec<Connection>>()?;
                        }
                        "sources" => {
                            sources_value = access.next_value::<Vec<serde_yaml::Value>>()?;
                        }
                        "sql" => {
                            sql = access.next_value::<Option<String>>()?;
                        }
                        "endpoints" => {
                            endpoints_value = access.next_value::<Vec<serde_yaml::Value>>()?;
                        }
                        "home_dir" => {
                            home_dir = access.next_value::<String>()?;
                        }
                        "cache_dir" => {
                            cache_dir = access.next_value::<String>()?;
                        }
                        "cache_max_map_size" => {
                            cache_max_map_size = access.next_value::<Option<u64>>()?;
                        }
                        "app_buffer_size" => {
                            app_buffer_size = access.next_value::<Option<u32>>()?;
                        }
                        "file_buffer_capacity" => {
                            file_buffer_capacity = access.next_value::<Option<u64>>()?;
                        }
                        "commit_size" => {
                            commit_size = access.next_value::<Option<u32>>()?;
                        }
                        "commit_timeout" => {
                            commit_timeout = access.next_value::<Option<u64>>()?;
                        }
                        "telemetry" => {
                            telemetry = access.next_value::<Option<TelemetryConfig>>()?;
                        }
                        "cloud" => {
                            cloud = access.next_value::<Option<Cloud>>()?;
                        }
                        "err_threshold" => {
                            err_threshold = access.next_value::<Option<u32>>()?;
                        }
                        _ => {
                            access.next_value::<IgnoredAny>()?;
                        }
                    }
                }

                let result_sources: Result<Vec<Source>, A::Error> = sources_value
                    .iter()
                    .enumerate()
                    .map(|(idx, source_value)| -> Result<Source, A::Error> {
                        let connection_ref = source_value["connection"].to_owned();
                        if connection_ref.is_null() {
                            return Err(de::Error::custom(format!(
                                "sources[{idx:}]: missing connection ref"
                            )));
                        }
                        let connection_ref: super::source::Value = serde_yaml::from_value(
                            source_value["connection"].to_owned(),
                        )
                        .map_err(|err| {
                            de::Error::custom(format!(
                                "sources[{idx:}]: connection ref - {err:} "
                            ))
                        })?;
                        let super::source::Value::Ref(connection_name) = connection_ref;
                        let mut source: Source = serde_yaml::from_value(source_value.to_owned())
                            .map_err(|err| {
                                de::Error::custom(format!("sources[{idx:}]: {err:} "))
                            })?;
                        let connection = connections
                            .iter()
                            .find(|c| c.name == connection_name)
                            .ok_or_else(|| {
                                de::Error::custom(format!(
                                    "sources[{idx:}]: Cannot find Ref connection name: {connection_name:}"
                                ))
                            })?;
                        source.connection = Some(connection.to_owned());
                        Ok(source)
                    })
                    .collect();

                let sources = result_sources?;

                let endpoints = endpoints_value
                    .iter()
                    .enumerate()
                    .map(|(idx, endpoint_value)| -> Result<ApiEndpoint, A::Error> {
                        let endpoint: ApiEndpoint =
                            serde_yaml::from_value(endpoint_value.to_owned()).map_err(|err| {
                                de::Error::custom(format!("api_endpoints[{idx:}]: {err:} "))
                            })?;
                        Ok(endpoint)
                    })
                    .collect::<Result<Vec<ApiEndpoint>, A::Error>>()?;

                Ok(Config {
                    app_name,
                    home_dir,
                    cache_dir,
                    api,
                    connections,
                    sources,
                    endpoints,
                    sql,
                    flags,
                    cache_max_map_size,
                    app_buffer_size,
                    file_buffer_capacity,
                    commit_size,
                    commit_timeout,
                    telemetry,
                    cloud,
                    err_threshold,
                })
            }
        }

        deserializer.deserialize_map(ConfigVisitor)
    }
}
