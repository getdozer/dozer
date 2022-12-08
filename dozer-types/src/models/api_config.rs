use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct ApiIndex {
    pub primary_key: Vec<String>,
}
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, prost::Message)]
pub struct ApiConfig {
    #[prost(message, tag = "1")]
    pub rest: Option<ApiRest>,
    #[prost(message, tag = "2")]
    pub grpc: Option<ApiGrpc>,
    #[prost(bool, tag = "3")]
    pub auth: bool,
    #[prost(message, tag = "4")]
    pub internal: Option<ApiInternal>,
    #[prost(message, optional, tag = "5")]
    pub app_id: Option<String>,
    #[prost(message, optional, tag = "6")]
    pub id: Option<String>,
}
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, prost::Message)]
pub struct ApiRest {
    #[prost(uint32, tag = "1")]
    pub port: u32,
    #[prost(string, tag = "2")]
    pub url: String,
    #[prost(bool, tag = "3")]
    pub cors: bool,
}
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, prost::Message)]
pub struct ApiGrpc {
    #[prost(uint32, tag = "1")]
    pub port: u32,
    #[prost(string, tag = "2")]
    pub url: String,
    #[prost(bool, tag = "3")]
    pub cors: bool,
    #[prost(bool, tag = "4")]
    pub web: bool,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, prost::Message)]
pub struct ApiInternal {
    #[prost(uint32, tag = "1")]
    pub port: u32,
    #[prost(string, tag = "2")]
    pub host: String,
}
