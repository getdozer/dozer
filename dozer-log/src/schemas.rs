use std::collections::HashSet;

use dozer_types::{
    serde::{Deserialize, Serialize},
    types::{IndexDefinition, Schema},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(crate = "dozer_types::serde")]
pub struct EndpointSchema {
    pub path: String,
    pub schema: Schema,
    pub secondary_indexes: Vec<IndexDefinition>,
    pub enable_token: bool,
    pub enable_on_event: bool,
    pub connections: HashSet<String>,
}
