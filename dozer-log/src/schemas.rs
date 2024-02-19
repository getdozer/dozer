use std::collections::{HashMap, HashSet};

use dozer_types::{
    serde::{Deserialize, Serialize},
    types::{PortHandle, Schema},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(crate = "dozer_types::serde")]
pub struct SinkSchema {
    pub schemas: HashMap<PortHandle, Schema>,
    pub connections: HashSet<String>,
}
