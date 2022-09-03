use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum MasterOptions {
    Option0(super::MasterAppendOnlyOptions),
    Option1(super::MasterOverwriteOptions),
}


    