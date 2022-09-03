use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum TransactionalOption {
    Option0(super::TransactionalRetainPartialHistoryOptions),
    Option1(super::TransactionalRetainFullHistoryOptions),
}


    