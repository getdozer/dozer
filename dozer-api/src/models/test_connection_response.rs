use serde::{Deserialize, Serialize};


#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TestConnectionResponse {
    #[serde(rename = "table_info")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_info: Option<Vec<super::TableInfo>>,
}

    