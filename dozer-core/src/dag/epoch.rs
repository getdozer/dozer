use crate::dag::node::NodeHandle;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Epoch {
    pub id: u64,
    pub details: HashMap<NodeHandle, (u64, u64)>,
}

impl Epoch {
    pub fn new(id: u64, details: HashMap<NodeHandle, (u64, u64)>) -> Self {
        Self { id, details }
    }

    pub fn from(id: u64, node_handle: NodeHandle, txid: u64, seq_in_tx: u64) -> Self {
        Self {
            id,
            details: [(node_handle, (txid, seq_in_tx))].into_iter().collect(),
        }
    }
}

impl Display for Epoch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let details_str = self
            .details
            .iter()
            .map(|e| format!("{} -> {}:{}", e.0, e.1 .0, e.1 .1))
            .fold(String::new(), |a, b| a + ", " + b.as_str());
        f.write_str(format!("epoch: {}, details: {}", self.id, details_str).as_str())
    }
}

pub(crate) struct EpochManager {
    commit_max_ops_count: u32,
    commit_curr_ops_count: u32,
    commit_max_duration: Duration,
    commit_last: Instant,
    curr_epoch: u64,
}

impl EpochManager {
    pub fn new(commit_max_ops_count: u32, commit_max_duration: Duration) -> Self {
        Self {
            commit_max_ops_count,
            commit_curr_ops_count: 0,
            commit_max_duration,
            commit_last: Instant::now(),
            curr_epoch: 0,
        }
    }
}
