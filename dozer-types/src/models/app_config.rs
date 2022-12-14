use serde::{
    de::{IgnoredAny, Visitor},
    Deserialize, Deserializer, Serialize,
};

use super::{
    api_config::ApiConfig, api_endpoint::ApiEndpoint, connection::Connection, source::Source,
};
#[derive(Serialize, PartialEq, Eq, Clone, prost::Message)]
pub struct Config {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[prost(string, optional, tag = "1")]
    pub id: Option<String>,
    #[prost(string, tag = "2")]
    pub app_name: String,
    #[prost(message, tag = "3")]
    pub api: Option<ApiConfig>,
    #[prost(message, repeated, tag = "4")]
    pub connections: Vec<Connection>,
    #[prost(message, repeated, tag = "5")]
    pub sources: Vec<Source>,
    #[prost(message, repeated, tag = "6")]
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
                let mut id: Option<String> = None;
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
                        let connection_ref: super::source::Value =
                            serde_yaml::from_value(source_value["connection"].to_owned()).unwrap();
                        let super::source::Value::Ref(connection_name) = connection_ref;
                        let mut source: Source =
                            serde_yaml::from_value(source_value.to_owned()).unwrap();
                        source.connection = Some(
                            connections
                                .iter()
                                .find(|c| c.name == connection_name)
                                .unwrap_or_else(|| {
                                    panic!("Cannot find Ref connection name: {}", connection_name)
                                })
                                .to_owned(),
                        );
                        source
                    })
                    .collect();

                Ok(Config {
                    app_name,
                    api,
                    connections,
                    sources,
                    endpoints,
                    id,
                    ..Default::default()
                })
            }
        }

        deserializer.deserialize_map(ConfigVisitor)
    }
}
