use super::{
    api_config::ApiConfig, api_endpoint::ApiEndpoint, connection::Connection, flags::Flags,
    source::Source,
};
use crate::{constants::DEFAULT_HOME_DIR, models::api_config::default_api_config};
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

    #[prost(message, tag = "3")]
    /// Api server config related: port, host, etc
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api: Option<ApiConfig>,

    #[prost(message, repeated, tag = "4")]
    /// connections to databases: Eg: Postgres, Snowflake, etc
    pub connections: Vec<Connection>,

    #[prost(message, repeated, tag = "5")]
    /// sources to ingest data related to particular connection
    pub sources: Vec<Source>,

    #[prost(message, repeated, tag = "6")]
    /// api endpoints to expose
    pub endpoints: Vec<ApiEndpoint>,

    #[prost(string, optional, tag = "7")]
    #[serde(skip_serializing_if = "Option::is_none")]
    /// transformations to apply to source data in SQL format as multiple queries
    pub sql: Option<String>,

    #[prost(string, tag = "8")]
    #[serde(default = "default_home_dir")]
    ///directory for all process; Default: ~/.dozer
    pub home_dir: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[prost(message, tag = "9")]
    /// flags to enable/disable features
    pub flags: Option<Flags>,

    /// Cache lmdb max map size
    #[prost(uint64, optional, tag = "10")]
    #[serde(
        default = "default_cache_max_map_size",
        skip_serializing_if = "Option::is_none"
    )]
    pub cache_max_map_size: Option<u64>,

    /// Pipeline lmdb max map size
    #[prost(uint64, optional, tag = "11")]
    #[serde(
        default = "default_app_max_map_size",
        skip_serializing_if = "Option::is_none"
    )]
    pub app_max_map_size: Option<u64>,

    /// Pipeline buffer size
    #[prost(uint32, optional, tag = "12")]
    #[serde(
        default = "default_app_buffer_size",
        skip_serializing_if = "Option::is_none"
    )]
    pub app_buffer_size: Option<u32>,

    /// Commit size
    #[prost(uint32, optional, tag = "13")]
    #[serde(
        default = "default_commit_size",
        skip_serializing_if = "Option::is_none"
    )]
    pub commit_size: Option<u32>,

    /// Commit timeout
    #[prost(uint64, optional, tag = "14")]
    #[serde(
        default = "default_commit_timeout",
        skip_serializing_if = "Option::is_none"
    )]
    pub commit_timeout: Option<u64>,
}

pub fn default_home_dir() -> String {
    DEFAULT_HOME_DIR.to_owned()
}

pub fn default_cache_max_map_size() -> Option<u64> {
    Some(1024 * 1024 * 1024 * 1024)
}

pub fn default_app_max_map_size() -> Option<u64> {
    Some(1024 * 1024 * 1024 * 1024)
}

pub fn default_app_buffer_size() -> Option<u32> {
    Some(20_000)
}

pub fn default_commit_size() -> Option<u32> {
    Some(10_000)
}

pub fn default_commit_timeout() -> Option<u64> {
    Some(50)
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

                let mut app_name = "".to_owned();
                let mut sql = None;
                let mut home_dir: String = default_home_dir();

                let mut cache_max_map_size: Option<u64> = default_cache_max_map_size();
                let mut app_max_map_size: Option<u64> = default_app_max_map_size();
                let mut app_buffer_size: Option<u32> = default_app_buffer_size();
                let mut commit_size: Option<u32> = default_commit_size();
                let mut commit_timeout: Option<u64> = default_commit_timeout();

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
                        "cache_max_map_size" => {
                            cache_max_map_size = access.next_value::<Option<u64>>()?;
                        }
                        "app_max_map_size" => {
                            app_max_map_size = access.next_value::<Option<u64>>()?;
                        }
                        "app_buffer_size" => {
                            app_buffer_size = access.next_value::<Option<u32>>()?;
                        }
                        "commit_size" => {
                            commit_size = access.next_value::<Option<u32>>()?;
                        }
                        "commit_timeout" => {
                            commit_timeout = access.next_value::<Option<u64>>()?;
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
                    api,
                    connections,
                    sources,
                    endpoints,
                    sql,
                    home_dir,
                    flags,
                    cache_max_map_size,
                    app_max_map_size,
                    app_buffer_size,
                    commit_size,
                    commit_timeout,
                })
            }
        }

        deserializer.deserialize_map(ConfigVisitor)
    }
}
