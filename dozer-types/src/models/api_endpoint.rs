use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct ApiIndex {
    pub primary_key: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct ApiEndpoint {
    pub id: Option<String>,
    pub name: String,
    pub path: String,
    pub enable_rest: bool,
    pub enable_grpc: bool,
    pub sql: String,
    pub index: ApiIndex,
}
