use std::{
    fmt::{Display, Formatter},
    time::SystemTime,
};

use dozer_types::node::{NodeHandle, OpIdentifier, SourceStates};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EpochCommonInfo {
    pub id: u64,
    pub next_record_index_to_persist: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Epoch {
    pub common_info: EpochCommonInfo,
    pub details: SourceStates,
    pub decision_instant: SystemTime,
}

impl Epoch {
    pub fn new(
        id: u64,
        details: SourceStates,
        next_record_index_to_persist: Option<usize>,
        decision_instant: SystemTime,
    ) -> Self {
        Self {
            common_info: EpochCommonInfo {
                id,
                next_record_index_to_persist,
            },
            details,
            decision_instant,
        }
    }

    pub fn from(
        common_info: EpochCommonInfo,
        node_handle: NodeHandle,
        txid: u64,
        seq_in_tx: u64,
        decision_instant: SystemTime,
    ) -> Self {
        Self {
            common_info,
            details: [(node_handle, OpIdentifier::new(txid, seq_in_tx))]
                .into_iter()
                .collect(),
            decision_instant,
        }
    }
}

impl Display for Epoch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let details_str = self
            .details
            .iter()
            .map(|e| format!("{} -> {}:{}", e.0, e.1.txid, e.1.seq_in_tx))
            .fold(String::new(), |a, b| a + ", " + b.as_str());
        f.write_str(format!("epoch: {}, details: {}", self.common_info.id, details_str).as_str())
    }
}

mod manager;

pub use manager::{ClosedEpoch, EpochManager, EpochManagerOptions};
