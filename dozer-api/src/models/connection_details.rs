use serde::{Deserialize, Serialize};


#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ConnectionDetails {
    #[serde(rename = "database")]
    pub database: String,
    #[serde(rename = "host")]
    pub host: String,
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "port")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<String>,
    #[serde(rename = "user")]
    pub user: String,
}

    