use std::{
    fmt::{Display, Formatter},
    sync::Arc,
    time::SystemTime,
};

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

impl Display for Epoch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let source_states = self
            .common_info
            .source_states
            .iter()
            .map(|e| format!("{} -> {}:{}", e.0, e.1.txid, e.1.seq_in_tx))
            .fold(String::new(), |a, b| a + ", " + b.as_str());
        f.write_str(format!("epoch: {}, details: {}", self.common_info.id, source_states).as_str())
    }
}

mod manager;

pub use manager::{ClosedEpoch, EpochManager, EpochManagerOptions};

use crate::checkpoint::CheckpointWriter;
