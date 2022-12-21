use crate::dag::errors::ExecutionError;
use crate::dag::node::NodeHandle;
use std::collections::{HashMap, HashSet};
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
}

pub(crate) struct EpochManager {
    last_epoch_changed: Instant,
    last_epoch: Option<Epoch>,
    last_epoch_delivered: Option<HashSet<NodeHandle>>,
    curr_epoch_count: u64,
    timeout_threshold: Duration,
    epoch_count_threshold: u64,
    curr_epoch: u64,
    curr_epoch_details: HashMap<NodeHandle, (u64, u64)>,
}

impl EpochManager {
    pub fn new(
        timeout_threshold: Duration,
        count_threshold: u64,
        initial_states: HashMap<NodeHandle, (u64, u64)>,
    ) -> Self {
        Self {
            last_epoch_changed: Instant::now(),
            last_epoch: None,
            last_epoch_delivered: None,
            curr_epoch_count: 0,
            timeout_threshold,
            epoch_count_threshold: count_threshold,
            curr_epoch: 0,
            curr_epoch_details: initial_states,
        }
    }

    fn close_current_epoch(&mut self) {
        self.last_epoch = Some(Epoch::new(
            self.curr_epoch.clone(),
            self.curr_epoch_details.clone(),
        ));
        self.curr_epoch += 1;
        let _ = self.last_epoch_delivered.insert(HashSet::new());
        self.last_epoch_changed = Instant::now();
        self.curr_epoch_count = 0;
    }

    pub fn poll_epoch(&self, source: &NodeHandle) -> Option<Epoch> {
        if self.last_epoch_delivered.contains(source) {
            None
        } else {
            self.last_epoch.clone()
        }
    }

    pub fn close_and_poll_epoch(&mut self, source: &NodeHandle) -> Option<Epoch> {
        self.close_current_epoch();
        self.poll_epoch(source)
    }

    pub fn add_op_to_epoch(
        &mut self,
        source: &NodeHandle,
        tx_id: u64,
        seq_in_tx: u64,
    ) -> Result<Option<Epoch>, ExecutionError> {
        self.curr_epoch_count += 1;
        let mut epoch_details = self
            .curr_epoch_details
            .get_mut(source)
            .ok_or(ExecutionError::InvalidNodeHandle(source.clone()))?;

        epoch_details.0 = tx_id;
        epoch_details.1 = seq_in_tx;

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
