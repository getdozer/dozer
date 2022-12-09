use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct ApiIndex {
    pub primary_key: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Default)]
pub struct ApiConfig {
    pub rest: ApiRest,
    pub grpc: ApiGrpc,
    pub auth: bool,
    pub internal: ApiInternal,
}
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Default)]
pub struct ApiRest {
    pub port: u16,
    pub url: String,
    pub cors: bool,
}
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Default)]
pub struct ApiGrpc {
    pub port: u16,
    pub url: String,
    pub cors: bool,
    pub web: bool,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Default)]
pub struct ApiInternal {
    pub port: u16,
    pub host: String,
}
