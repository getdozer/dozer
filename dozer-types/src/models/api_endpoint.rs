use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct ApiEndpoint {
    pub id: Option<String>,
    pub name: String,
    pub path: String,
    pub enable_rest: bool,
    pub enable_grpc: bool,
    pub sql: String,
}
