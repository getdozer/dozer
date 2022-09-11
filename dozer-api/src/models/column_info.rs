use serde::{Deserialize, Serialize};


#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ColumnInfo {
    #[serde(rename = "column_name")]
    pub column_name: String,
    #[serde(rename = "is_nullable")]
    pub is_nullable: bool,
    #[serde(rename = "is_primary_key")]
    pub is_primary_key: bool,
    #[serde(rename = "udt_name")]
    pub udt_name: String,
}

    