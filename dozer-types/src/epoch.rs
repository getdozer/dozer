use std::{sync::Arc, time::SystemTime};

use crate::node::SourceStates;

#[derive(Clone, Debug)]
pub struct EpochCommonInfo {
    pub id: u64,
    pub source_states: Arc<SourceStates>,
}

#[derive(Clone, Debug)]
pub struct Epoch {
    pub common_info: EpochCommonInfo,
    pub decision_instant: SystemTime,
}

impl Epoch {
    pub fn new(id: u64, source_states: Arc<SourceStates>, decision_instant: SystemTime) -> Self {
        Self {
            common_info: EpochCommonInfo { id, source_states },
            decision_instant,
        }
    }

    pub fn from(common_info: EpochCommonInfo, decision_instant: SystemTime) -> Self {
        Self {
            common_info,
            decision_instant,
        }
    }
}
