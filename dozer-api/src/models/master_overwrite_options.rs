use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum MasterOverwriteOptionsMasterSourceOptionType {
    #[serde(rename = "overwrite")]
    Overwrite,
}

impl std::fmt::Display for MasterOverwriteOptionsMasterSourceOptionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}",
            match self {
                MasterOverwriteOptionsMasterSourceOptionType::Overwrite => "overwrite",
            }
        )
    }
}

impl std::str::FromStr for MasterOverwriteOptionsMasterSourceOptionType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "overwrite" => Ok(MasterOverwriteOptionsMasterSourceOptionType::Overwrite),
            _ => Err(format!("'{}' is not a valid value for MasterOverwriteOptionsMasterSourceOptionType", s)),
        }
    }
}


#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MasterOverwriteOptions {
    #[serde(rename = "master_source_option_type")]
    pub master_source_option_type: MasterOverwriteOptionsMasterSourceOptionType,
    #[serde(rename = "override")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#override: Option<String>,
}

    