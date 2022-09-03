use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum SourceDataLayout {
    Option0(super::MasterSourceDataLayout),
    Option1(super::TransactionalSourceDataLayout),
}


    