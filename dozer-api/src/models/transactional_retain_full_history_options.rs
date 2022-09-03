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


#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TransactionalRetainFullHistoryOptions {
    #[serde(rename = "transactional_source_option_type")]
    pub transactional_source_option_type: TransactionalRetainFullHistoryOptionsTransactionalSourceOptionType,
}

    