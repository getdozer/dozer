use super::{
    api_config::ApiConfig, api_endpoint::ApiEndpoint, connection::Connection, source::Source,
};
use crate::{constants::DEFAULT_HOME_DIR, models::api_config::default_api_config};
use serde::{
    de::{self, IgnoredAny, Visitor},
    Deserialize, Deserializer, Serialize,
};
#[derive(Serialize, PartialEq, Eq, Clone, prost::Message)]
/// The configuration for the app
pub struct Config {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[prost(string, optional, tag = "1")]
    pub id: Option<String>,
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
    #[prost(string, tag = "7")]
    #[serde(default = "default_home_dir")]
    ///directory for all process; Default: ~/.dozer
    pub home_dir: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[prost(message, tag = "8")]
    /// flags to enable/disable features
    pub flags: Option<Flags>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, prost::Message)]
pub struct Flags {
    /// dynamic grpc enabled; Default: true
    #[prost(bool, tag = "1", default = true)]
    pub dynamic: bool,
    /// http1 + web support for grpc. This is required for browser clients.; Default: true
    #[prost(bool, tag = "2", default = true)]
    pub grpc_web: bool,
    /// push events enabled. Currently unstable.; Default: false
    #[prost(bool, tag = "3", default = false)]
    pub push_events: bool,
}

pub fn default_home_dir() -> String {
    DEFAULT_HOME_DIR.to_owned()
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
                let mut flags: Option<Flags> = Some(Default::default());
                let mut connections: Vec<Connection> = vec![];
                let mut sources_value: Vec<serde_yaml::Value> = vec![];
                let mut endpoints: Vec<ApiEndpoint> = vec![];
                let mut app_name = "".to_owned();
                let mut id: Option<String> = None;
                let mut home_dir: String = default_home_dir();
                while let Some(key) = access.next_key()? {
                    match key {
                        "id" => {
                            id = access.next_value::<Option<String>>()?;
                        }
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
                        "endpoints" => {
                            endpoints = access.next_value::<Vec<ApiEndpoint>>()?;
                        }
                        "home_dir" => {
                            home_dir = access.next_value::<String>()?;
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
                                "sources[{:}]: missing connection ref",
                                idx
                            )));
                        }
                        let connection_ref: super::source::Value = serde_yaml::from_value(
                            source_value["connection"].to_owned(),
                        )
                        .map_err(|err| {
                            de::Error::custom(format!(
                                "sources[{:}]: connection ref - {:} ",
                                idx, err
                            ))
                        })?;
                        let super::source::Value::Ref(connection_name) = connection_ref;
                        let mut source: Source = serde_yaml::from_value(source_value.to_owned())
                            .map_err(|err| {
                                de::Error::custom(format!("sources[{:}]: {:} ", idx, err))
                            })?;
                        let connection = connections
                            .iter()
                            .find(|c| c.name == connection_name)
                            .ok_or_else(|| {
                                de::Error::custom(format!(
                                    "sources[{:}]: Cannot find Ref connection name: {:}",
                                    idx, connection_name
                                ))
                            })?;
                        source.connection = Some(connection.to_owned());
                        Ok(source)
                    })
                    .collect();
                let sources = result_sources?;
                Ok(Config {
                    id,
                    app_name,
                    api,
                    connections,
                    sources,
                    endpoints,
                    home_dir,
                    flags,
                })
            }
        }

        deserializer.deserialize_map(ConfigVisitor)
    }
}
