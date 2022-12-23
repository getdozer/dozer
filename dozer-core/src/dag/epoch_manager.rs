use crate::dag::errors::ExecutionError;
use crate::dag::node::NodeHandle;
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter, Write};
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

pub struct EpochManager {
    last_epoch_changed: Instant,
    last_epoch: Option<(Epoch, HashSet<NodeHandle>)>,
    curr_ops_in_epoch: u64,
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
            curr_ops_in_epoch: 0,
            timeout_threshold,
            epoch_count_threshold: count_threshold,
            curr_epoch: 0,
            curr_epoch_details: initial_states,
        }
    }

    fn close_current_epoch(&mut self, source: &NodeHandle) -> Epoch {
        //
        let closed_epoch = Epoch::new(self.curr_epoch.clone(), self.curr_epoch_details.clone());
        self.last_epoch = Some((closed_epoch.clone(), HashSet::from([source.clone()])));
        self.curr_epoch += 1;
        self.last_epoch_changed = Instant::now();
        self.curr_ops_in_epoch = 0;
        closed_epoch
    }

    pub fn poll_epoch(&mut self, source: &NodeHandle) -> Option<Epoch> {
        if let Some((epoch, receivers)) = self.last_epoch.as_mut() {
            if receivers.contains(source) {
                None
            } else {
                receivers.insert(source.clone());
                Some(epoch.clone())
            }
        } else {
            None
        }
    }

    pub fn close_and_poll_epoch(&mut self, source: &NodeHandle) -> Option<Epoch> {
        Some(self.close_current_epoch(source))
    }

    fn should_start_new_epoch(&self) -> bool {
        let thresholds_elapsed = self
            .last_epoch_changed
            .elapsed()
            .gt(&self.timeout_threshold)
            || self.curr_ops_in_epoch >= self.epoch_count_threshold;

        match (thresholds_elapsed, self.last_epoch.as_ref()) {
            (true, Some((epoch, receiver))) => {
                if receiver.len() == self.curr_epoch_details.len() {
                    true
                } else {
                    false
                }
            }
            (true, None) => true,
            (false, _) => false,
        }
    }

    pub fn add_op_to_epoch(
        &mut self,
        source: &NodeHandle,
        tx_id: u64,
        seq_in_tx: u64,
    ) -> Result<Option<Epoch>, ExecutionError> {
        self.curr_ops_in_epoch += 1;
        let mut epoch_details = self
            .curr_epoch_details
            .get_mut(source)
            .ok_or(ExecutionError::InvalidNodeHandle(source.clone()))?;

        epoch_details.0 = tx_id;
        epoch_details.1 = seq_in_tx;

        Ok(if self.should_start_new_epoch() {
            Some(self.close_current_epoch(source))
        } else {
            self.poll_epoch(source)
        })
    }
}
