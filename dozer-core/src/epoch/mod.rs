use std::{sync::Arc, time::SystemTime};

use dozer_types::node::SourceStates;

#[derive(Clone, Debug)]
pub struct EpochCommonInfo {
    pub id: u64,
    pub checkpoint_writer: Option<Arc<CheckpointWriter>>,
    pub source_states: Arc<SourceStates>,
}

#[derive(Clone, Debug)]
pub struct Epoch {
    pub common_info: EpochCommonInfo,
    pub decision_instant: SystemTime,
}

impl Epoch {
    pub fn new(
        id: u64,
        source_states: Arc<SourceStates>,
        checkpoint_writer: Option<Arc<CheckpointWriter>>,
        decision_instant: SystemTime,
    ) -> Self {
        Self {
            common_info: EpochCommonInfo {
                id,
                checkpoint_writer,
                source_states,
            },
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

mod manager;

pub use manager::{ClosedEpoch, EpochManager, EpochManagerOptions};

use crate::checkpoint::CheckpointWriter;
