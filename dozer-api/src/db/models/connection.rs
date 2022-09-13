use serde::{Deserialize, Serialize};

use crate::models::{ConnectionAuthentication, ConnectionResponse, ConnectionType};
#[derive(Queryable, Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Connection {
    pub id: String,
    pub auth: String,
    pub db_type: String,
}

impl From<Connection> for ConnectionResponse {
    fn from(input: Connection) -> Self {
        ConnectionResponse {
            authentication: serde_json::from_str::<ConnectionAuthentication>(&input.auth).unwrap(),
            id: input.id,
            r#type: input.db_type.parse::<ConnectionType>().unwrap(),
        }
    }
}
