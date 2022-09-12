use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum TransactionalRetainPartialHistoryOptionsTransactionalSourceOptionType {
    #[serde(rename = "retain_partial_history")]
    RetainPartialHistory,
}

impl std::fmt::Display for TransactionalRetainPartialHistoryOptionsTransactionalSourceOptionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}",
            match self {
                TransactionalRetainPartialHistoryOptionsTransactionalSourceOptionType::RetainPartialHistory => "retain_partial_history",
            }
        )
    }
}

impl std::str::FromStr for TransactionalRetainPartialHistoryOptionsTransactionalSourceOptionType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "retain_partial_history" => Ok(TransactionalRetainPartialHistoryOptionsTransactionalSourceOptionType::RetainPartialHistory),
            _ => Err(format!("'{}' is not a valid value for TransactionalRetainPartialHistoryOptionsTransactionalSourceOptionType", s)),
        }
    }
}


#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TransactionalRetainPartialHistoryOptions {
    #[serde(rename = "retention_period")]
    pub retention_period: f64,
    #[serde(rename = "timestamp_field")]
    pub timestamp_field: String,
    #[serde(rename = "transactional_source_option_type")]
    pub transactional_source_option_type: TransactionalRetainPartialHistoryOptionsTransactionalSourceOptionType,
}

    