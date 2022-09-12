use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum MasterSourceDataLayoutSourceDataType {
    #[serde(rename = "master")]
    Master,
}

impl std::fmt::Display for MasterSourceDataLayoutSourceDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}",
            match self {
                MasterSourceDataLayoutSourceDataType::Master => "master",
            }
        )
    }
}

impl std::str::FromStr for MasterSourceDataLayoutSourceDataType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "master" => Ok(MasterSourceDataLayoutSourceDataType::Master),
            _ => Err(format!("'{}' is not a valid value for MasterSourceDataLayoutSourceDataType", s)),
        }
    }
}


#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MasterSourceDataLayout {
    #[serde(rename = "options")]
    pub options: super::MasterOptions,
    #[serde(rename = "source_data_type")]
    pub source_data_type: MasterSourceDataLayoutSourceDataType,
}

    