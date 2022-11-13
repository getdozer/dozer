use crate::types::{Operation, Schema};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ApiEvent {
    SchemaChange(String, Schema),
    Operation(String, Operation),
}
