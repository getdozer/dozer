use serde::{Deserialize, Serialize};


#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TableInfo {
    #[serde(rename = "columns")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<super::ColumnInfo>>,
    #[serde(rename = "table_name")]
    pub table_name: String,
}

    