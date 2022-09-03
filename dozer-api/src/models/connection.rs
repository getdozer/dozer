use serde::{Deserialize, Serialize};


#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Queryable)]
#[serde(deny_unknown_fields)]
pub struct Connection {
    #[serde(rename = "id")]
    pub id: i32,
    #[serde(rename = "auth")]
    pub auth: crate::db::types::connection_details_json_type::ConnectionDetailsJsonType,
    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
}