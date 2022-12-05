use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct ApiIndex {
    pub primary_key: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct ApiConfig {
    pub rest: ApiRest,
    pub grpc: ApiGrpc,
    pub auth: bool,
    pub internal: ApiInternal,
}
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct ApiRest {
    pub port: u32,
    pub url: String,
    pub cors: bool,
}
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct ApiGrpc {
    pub port: u32,
    pub url: String,
    pub cors: bool,
    pub web: bool,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct ApiInternal {
    pub port: u32,
    pub host: String,
}
