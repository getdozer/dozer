use crate::dag::errors::ExecutionError;
use crate::dag::node::NodeHandle;
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Barrier};
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

pub struct ClosingEpoch {
    pub id: u64,
    pub barrier: Arc<Barrier>,
    pub forced: bool,
}

impl ClosingEpoch {
    pub fn new(id: u64, barrier: Arc<Barrier>, forced: bool) -> Self {
        Self {
            id,
            barrier,
            forced,
        }
    }
}

pub(crate) struct EpochManager {
    commit_max_ops_count: u32,
    commit_curr_ops_count: u32,
    commit_max_duration: Duration,
    commit_last: Instant,
    curr_epoch: u64,
    epoch_participants: HashMap<NodeHandle, bool>,
    epoch_barrier: Arc<Barrier>,
    epoch_closing: bool,
    epoch_forced: bool,
}

impl EpochManager {
    pub fn new(
        commit_max_ops_count: u32,
        commit_max_duration: Duration,
        epoch_participants: HashSet<NodeHandle>,
    ) -> Self {
        let participants_count = epoch_participants.len();

        Self {
            commit_max_ops_count,
            commit_curr_ops_count: 0,
            commit_max_duration,
            commit_last: Instant::now(),
            curr_epoch: 0,
            epoch_participants: epoch_participants.into_iter().map(|e| (e, false)).collect(),
            epoch_closing: false,
            epoch_barrier: Arc::new(Barrier::new(participants_count)),
            epoch_forced: false,
        }
    }

    #[inline]
    fn should_close_epoch(&mut self) -> bool {
        (self.commit_curr_ops_count > 0 && self.commit_last.elapsed().gt(&self.commit_max_duration))
            || (self.commit_curr_ops_count >= self.commit_max_ops_count)
    }

    fn close_epoch_if_possible(&mut self) -> bool {
        if self.epoch_participants.iter().all(|b| *b.1) {
            self.epoch_closing = false;
            self.commit_curr_ops_count = 0;
            self.curr_epoch += 1;
            self.commit_last = Instant::now();
            self.epoch_barrier = Arc::new(Barrier::new(self.epoch_participants.len()));
            self.epoch_participants
                .values_mut()
                .for_each(|value| *value = false);
            true
        } else {
            false
        }
    }

    pub fn advance(
        &mut self,
        participant: &NodeHandle,
        advance_count: u16,
        force: bool,
    ) -> Result<Option<ClosingEpoch>, ExecutionError> {
        //
        self.commit_curr_ops_count += advance_count as u32;

        // Check if epoch is already in a closing state.
        match self.epoch_closing {
            false => {
                // If it is not, we check if we should close the current epoch
                if force || self.should_close_epoch() {
                    self.epoch_forced = force;
                    let curr_participant_state = self
                        .epoch_participants
                        .get_mut(participant)
                        .ok_or_else(|| ExecutionError::InvalidNodeHandle(participant.clone()))?;
                    assert!(!*curr_participant_state);

                    // If we can close it, we add the current participant in the participants list
                    self.epoch_closing = true;
                    *curr_participant_state = true;
                    let closing_epoch = ClosingEpoch::new(
                        self.curr_epoch,
                        self.epoch_barrier.clone(),
                        self.epoch_forced,
                    );
                    // Epoch might have a single participant. We check for it and, if true, we close
                    // the current epoch, otherwise we leave it open and wait for other participants
                    // to join teh closing
                    self.close_epoch_if_possible();
                    Ok(Some(closing_epoch))
                } else {
                    Ok(None)
                }
            }
            // if it is, we have to ensure all participants are notified before proceeding
            // to the next epoch.
            true => {
                if !force {
                    assert!(!self.epoch_forced, "Epoch is already closing with a forced flag but someone believes otherwise");
                }

                let curr_participant_state = self
                    .epoch_participants
                    .get_mut(participant)
                    .ok_or_else(|| ExecutionError::InvalidNodeHandle(participant.clone()))?;
                assert!(!*curr_participant_state);

                *curr_participant_state = true;
                let closing_epoch = ClosingEpoch::new(
                    self.curr_epoch,
                    self.epoch_barrier.clone(),
                    self.epoch_forced,
                );
                self.close_epoch_if_possible();
                Ok(Some(closing_epoch))
            }
        }
    }
}
