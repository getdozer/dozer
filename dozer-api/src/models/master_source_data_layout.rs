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


#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MasterSourceDataLayout {
    #[serde(rename = "options")]
    pub options: super::MasterOptions,
    #[serde(rename = "source_data_type")]
    pub source_data_type: MasterSourceDataLayoutSourceDataType,
}

    