use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message)]
pub struct ApiIndex {
    #[prost(string, repeated, tag = "1")]
    pub primary_key: Vec<String>,
}

#[derive(Deserialize, Eq, PartialEq, Clone, ::prost::Message)]
pub struct ApiEndpoint {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    /// name of the table in source database; Type: String
    pub table_name: String,

    #[prost(string, tag = "3")]
    /// path of endpoint - e.g: /stocks
    pub path: String,
    #[prost(message, tag = "4")]
    pub index: Option<ApiIndex>,
}

impl Serialize for ApiEndpoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ApiEndpoint", 3)?;
        state.serialize_field("name", &self.name)?;
        state.serialize_field("path", &self.path)?;
        state.serialize_field("index", &self.index)?;
        state.serialize_field("table_name", &self.table_name)?;

        state.end()
    }
}
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum Value {
    Ref(String),
}
