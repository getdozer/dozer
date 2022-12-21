use crate::dag::errors::ExecutionError;
use crate::dag::node::NodeHandle;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct EpochDetails {
    pub tx_id: u64,
    pub seq_in_tx: u64,
}

impl EpochDetails {
    pub fn new(tx_id: u64, seq_in_tx: u64) -> Self {
        Self { tx_id, seq_in_tx }
    }
}

#[derive(Clone)]
pub struct Epoch {
    id: u64,
    details: HashMap<NodeHandle, EpochDetails>,
}

impl Epoch {
    pub fn new(id: u64, details: HashMap<NodeHandle, EpochDetails>) -> Self {
        Self { id, details }
    }
}

pub(crate) struct EpochsManager {
    last_epoch_changed: Instant,
    last_epoch: Option<Epoch>,
    last_epoch_delivered: HashSet<NodeHandle>,
    curr_epoch_count: u64,
    timeout_threshold: Duration,
    epoch_count_threshold: u64,
    curr_epoch: u64,
    curr_epoch_details: HashMap<NodeHandle, EpochDetails>,
}

impl EpochsManager {
    pub fn new(
        timeout_threshold: Duration,
        count_threshold: u64,
        initial_states: HashMap<NodeHandle, EpochDetails>,
    ) -> Self {
        Self {
            last_epoch_changed: Instant::now(),
            last_epoch: None,
            last_epoch_delivered: HashSet::new(),
            curr_epoch_count: 0,
            timeout_threshold,
            epoch_count_threshold: count_threshold,
            curr_epoch: 0,
            curr_epoch_details: initial_states,
        }
    }

    fn close_current_epoch(&mut self) {
        self.last_epoch = Some(Epoch::new(self.curr_epoch, self.curr_epoch_details.clone()));
        self.curr_epoch += 1;
        self.last_epoch_delivered.clear();
        self.last_epoch_changed = Instant::now();
        self.curr_epoch_count = 0;
    }

    pub fn poll_epoch(&self, source: &NodeHandle) -> &Option<Epoch> {
        if self.last_epoch_delivered.contains(source) {
            &None
        } else {
            &self.last_epoch
        }
    }

    pub fn add_op_to_epoch(
        &mut self,
        source: &NodeHandle,
        tx_id: u64,
        seq_in_tx: u64,
    ) -> Result<&Option<Epoch>, ExecutionError> {
        self.curr_epoch_count += 1;
        let mut epoch_details = self
            .curr_epoch_details
            .get_mut(source)
            .ok_or(ExecutionError::InvalidNodeHandle(source.clone()))?;

        epoch_details.tx_id = tx_id;
        epoch_details.seq_in_tx = seq_in_tx;

        if (self
            .last_epoch_changed
            .elapsed()
            .gt(&self.timeout_threshold)
            || self.curr_epoch_count >= self.epoch_count_threshold)
            && self.last_epoch_delivered.len() == self.curr_epoch_details.len()
        {
            self.close_current_epoch();
        }
        Ok(self.poll_epoch(source))
    }
}
