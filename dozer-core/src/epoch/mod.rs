use std::{sync::Arc, time::SystemTime};

use dozer_log::storage::Queue;
use dozer_types::node::SourceStates;

#[derive(Clone, Debug)]
pub struct EpochCommonInfo {
    pub id: u64,
    pub checkpoint_writer: Option<Arc<CheckpointWriter>>,
    pub sink_persist_queue: Option<Queue>,
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
        sink_persist_queue: Option<Queue>,
        decision_instant: SystemTime,
    ) -> Self {
        Self {
            common_info: EpochCommonInfo {
                id,
                checkpoint_writer,
                sink_persist_queue,
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
