use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct ApiEndpoint {
    id: Option<String>,
    name: String,
    path: String,
    enable_rest: bool,
    enable_grpc: bool,
    sql: String,
}
