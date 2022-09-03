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


#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TransactionalSourceDataLayout {
    #[serde(rename = "options")]
    pub options: super::TransactionalOption,
    #[serde(rename = "source_data_type")]
    pub source_data_type: TransactionalSourceDataLayoutSourceDataType,
}

    