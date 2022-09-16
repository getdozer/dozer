use serde::{Deserialize, Serialize};

#[derive(Queryable, Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Connection {
    pub id: String,
    pub auth: String,
    pub db_type: String,
}
