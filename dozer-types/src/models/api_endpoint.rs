use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message)]
pub struct ApiIndex {
    #[prost(string, repeated, tag = "1")]
    pub primary_key: Vec<String>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message)]
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
}
