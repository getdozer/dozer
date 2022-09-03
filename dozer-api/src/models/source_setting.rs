use serde::{Deserialize, Serialize};


#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SourceSetting {
    #[serde(rename = "connection_id")]
    pub connection_id: f64,
    #[serde(rename = "connection_table_name")]
    pub connection_table_name: String,
    #[serde(rename = "data_layout")]
    pub data_layout: super::SourceDataLayout,
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "refresh_options")]
    pub refresh_options: super::RefreshOptions,
    #[serde(rename = "table_name")]
    pub table_name: String,
}

    