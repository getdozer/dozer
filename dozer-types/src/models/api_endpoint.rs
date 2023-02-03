use crate::models::source::Source;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message)]
pub struct ApiIndex {
    #[prost(string, repeated, tag = "1")]
    pub primary_key: Vec<String>,
}

#[derive(Deserialize, Eq, PartialEq, Clone, ::prost::Message)]
pub struct ApiEndpoint {
    #[prost(string, optional, tag = "1")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[prost(string, optional, tag = "2")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_id: Option<String>,
    #[prost(string, tag = "3")]
    pub name: String,
    #[prost(string, tag = "4")]
    /// path of endpoint - e.g: /stocks
    pub path: String,
    #[prost(string, optional, tag = "5")]
    pub sql: Option<String>,
    #[prost(message, tag = "6")]
    pub index: Option<ApiIndex>,
    #[prost(message, tag = "7")]
    #[serde(skip_deserializing)]
    /// reference to pre-defined source name - syntax: `!Ref <source_name>`; Type: `Ref!` tag
    pub source: Option<Source>,
}

impl Serialize for ApiEndpoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ApiEndpoint", 3)?;
        state.serialize_field("name", &self.name)?;
        state.serialize_field("path", &self.path)?;
        state.serialize_field("sql", &self.sql)?;
        state.serialize_field("index", &self.index)?;
        if let Some(source) = &self.source {
            state.serialize_field("source", &Value::Ref(source.name.clone()))?;
        }
        state.end()
    }
}
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum Value {
    Ref(String),
}
