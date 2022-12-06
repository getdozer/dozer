pub mod types;
use super::OrchestrationError;
use dozer_types::models::{
    api_config::ApiConfig, api_endpoint::ApiEndpoint, connection::Connection, source::Source,
};
use serde::{
    de::{IgnoredAny, Visitor},
    Deserialize, Deserializer, Serialize,
};
use std::fs;
#[derive(Serialize, Debug, PartialEq, Eq)]
pub struct Config {
    pub app_name: String,
    pub api: ApiConfig,
    pub connections: Vec<Connection>,
    pub sources: Vec<Source>,
    pub endpoints: Vec<ApiEndpoint>,
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
                let mut api: Option<ApiConfig> = None;
                let mut connections: Vec<Connection> = vec![];
                let mut sources_value: Vec<serde_yaml::Value> = vec![];
                let mut endpoints: Vec<ApiEndpoint> = vec![];
                let mut app_name = "".to_owned();
                while let Some(key) = access.next_key()? {
                    match key {
                        "app_name" => {
                            app_name = access.next_value::<String>()?;
                        }
                        "api" => {
                            api = Some(access.next_value::<ApiConfig>()?);
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
                        _ => {
                            access.next_value::<IgnoredAny>()?;
                        }
                    }
                }

                let sources = sources_value
                    .iter()
                    .map(|source_value| {
                        let connection_ref: dozer_types::models::source::Value =
                            serde_yaml::from_value(source_value["connection"].to_owned()).unwrap();
                        let dozer_types::models::source::Value::Ref(connection_name) =
                            connection_ref;
                        let mut source: Source =
                            serde_yaml::from_value(source_value.to_owned()).unwrap();
                        source.connection = connections
                            .iter()
                            .find(|c| c.name == connection_name)
                            .unwrap_or_else(|| {
                                panic!("Cannot find Ref connection name: {}", connection_name)
                            })
                            .to_owned();
                        source
                    })
                    .collect();

                Ok(Config {
                    app_name,
                    api: api.unwrap(),
                    connections,
                    sources,
                    endpoints,
                })
            }
        }

        deserializer.deserialize_map(ConfigVisitor)
    }
}

pub fn load_config(config_path: String) -> Result<Config, OrchestrationError> {
    let contents = fs::read_to_string(config_path).map_err(OrchestrationError::FailedToLoadFile)?;
    let config = serde_yaml::from_str(&contents)
        .map_err(|e| OrchestrationError::FailedToParseYaml(Box::new(e)))?;
    Ok(config)
}
