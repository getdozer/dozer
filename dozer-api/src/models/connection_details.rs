use serde::{Deserialize, Serialize};


#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ConnectionDetails {
    #[serde(rename = "database")]
    pub database: String,
    #[serde(rename = "host")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(rename = "user")]
    pub user: String,
}

    