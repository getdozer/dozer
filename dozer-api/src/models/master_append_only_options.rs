use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum MasterAppendOnlyOptionsMasterSourceOptionType {
    #[serde(rename = "append_only")]
    AppendOnly,
}

impl std::fmt::Display for MasterAppendOnlyOptionsMasterSourceOptionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}",
            match self {
                MasterAppendOnlyOptionsMasterSourceOptionType::AppendOnly => "append_only",
            }
        )
    }
}


#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MasterAppendOnlyOptions {
    #[serde(rename = "closed_date_field")]
    pub closed_date_field: String,
    #[serde(rename = "master_source_option_type")]
    pub master_source_option_type: MasterAppendOnlyOptionsMasterSourceOptionType,
    #[serde(rename = "open_date_field")]
    pub open_date_field: String,
    #[serde(rename = "unique_key_field")]
    pub unique_key_field: String,
}

    