use serde::{Deserialize, Serialize};


#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ConnectionResponse {
    #[serde(rename = "authentication")]
    pub authentication: super::ConnectionAuthentication,
    #[serde(rename = "id")]
    pub id: String,
    #[serde(rename = "type")]
    pub r#type: super::ConnectionType,
}

    