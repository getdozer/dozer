use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum TransactionalSourceDataLayoutSourceDataType {
    #[serde(rename = "transactional")]
    Transactional,
}

impl std::fmt::Display for TransactionalSourceDataLayoutSourceDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}",
            match self {
                TransactionalSourceDataLayoutSourceDataType::Transactional => "transactional",
            }
        )
    }
}

impl std::str::FromStr for TransactionalSourceDataLayoutSourceDataType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "transactional" => Ok(TransactionalSourceDataLayoutSourceDataType::Transactional),
            _ => Err(format!("'{}' is not a valid value for TransactionalSourceDataLayoutSourceDataType", s)),
        }
    }
}


#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TransactionalSourceDataLayout {
    #[serde(rename = "options")]
    pub options: super::TransactionalOption,
    #[serde(rename = "source_data_type")]
    pub source_data_type: TransactionalSourceDataLayoutSourceDataType,
}

    