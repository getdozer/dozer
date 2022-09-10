use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum TransactionalRetainFullHistoryOptionsTransactionalSourceOptionType {
    #[serde(rename = "retain_full_history")]
    RetainFullHistory,
}

impl std::fmt::Display for TransactionalRetainFullHistoryOptionsTransactionalSourceOptionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}",
            match self {
                TransactionalRetainFullHistoryOptionsTransactionalSourceOptionType::RetainFullHistory => "retain_full_history",
            }
        )
    }
}

impl std::str::FromStr for TransactionalRetainFullHistoryOptionsTransactionalSourceOptionType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "retain_full_history" => Ok(TransactionalRetainFullHistoryOptionsTransactionalSourceOptionType::RetainFullHistory),
            _ => Err(format!("'{}' is not a valid value for TransactionalRetainFullHistoryOptionsTransactionalSourceOptionType", s)),
        }
    }
}


#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TransactionalRetainFullHistoryOptions {
    #[serde(rename = "transactional_source_option_type")]
    pub transactional_source_option_type: TransactionalRetainFullHistoryOptionsTransactionalSourceOptionType,
}

    