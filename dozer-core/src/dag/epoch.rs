use crate::dag::node::NodeHandle;
use dozer_types::parking_lot::Mutex;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Barrier};
use std::thread::sleep;
use std::time::Duration;

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

#[derive(Debug, Clone)]
pub struct ClosingEpoch {
    pub id: u64,
    pub details: HashMap<NodeHandle, (u64, u64)>,
    pub terminating: bool,
}

impl ClosingEpoch {
    pub fn new(id: u64, details: HashMap<NodeHandle, (u64, u64)>, terminating: bool) -> Self {
        Self {
            id,
            details,
            terminating,
        }
    }
}

#[derive(Debug)]
enum EpochManagerState {
    Closing {
        epoch: Epoch,
        should_terminate: bool,
        barrier: Arc<Barrier>,
    },
    Closed {
        epoch: ClosingEpoch,
        num_participant_confirmations: usize,
    },
}

#[derive(Debug)]
pub(crate) struct EpochManager {
    num_participants: usize,
    state: Mutex<EpochManagerState>,
}

impl EpochManager {
    pub fn new(num_participants: usize) -> Self {
        Self {
            num_participants,
            state: Mutex::new(EpochManagerState::Closing {
                epoch: Epoch::new(0, HashMap::new()),
                should_terminate: true,
                barrier: Arc::new(Barrier::new(num_participants)),
            }),
        }
    }

    pub fn wait_for_epoch_close(
        &self,
        participant: NodeHandle,
        txn_id_and_seq_number: (u64, u64),
        request_termination: bool,
    ) -> ClosingEpoch {
        let barrier = loop {
            let mut state = self.state.lock();
            match &mut *state {
                EpochManagerState::Closing {
                    epoch,
                    should_terminate,
                    barrier,
                } => {
                    epoch.details.insert(participant, txn_id_and_seq_number);
                    // If anyone doesn't want to terminate, we don't terminate.
                    *should_terminate = *should_terminate && request_termination;
                    break barrier.clone();
                }
                EpochManagerState::Closed { .. } => {
                    // This thread wants to close a new epoch while some other thread hasn't got confirmation of last epoch closing.
                    // Just release the lock and put this thread to sleep.
                    drop(state);
                    sleep(Duration::from_millis(1));
                }
            }
        };

        barrier.wait();

        let mut state = self.state.lock();
        if let EpochManagerState::Closing {
            epoch,
            should_terminate,
            ..
        } = &mut *state
        {
            // This thread is the first one in this critical area.
            debug_assert!(epoch.details.len() == self.num_participants);
            let closing_epoch =
                ClosingEpoch::new(epoch.id, epoch.details.clone(), *should_terminate);
            *state = EpochManagerState::Closed {
                epoch: closing_epoch,
                num_participant_confirmations: 0,
            };
        }

        match &mut *state {
            EpochManagerState::Closed {
                epoch,
                num_participant_confirmations,
            } => {
                *num_participant_confirmations += 1;
                let closing_epoch = epoch.clone();
                if *num_participant_confirmations == self.num_participants {
                    // This thread is the last one in this critical area.
                    *state = EpochManagerState::Closing {
                        epoch: Epoch::new(closing_epoch.id + 1, HashMap::new()),
                        should_terminate: true,
                        barrier: Arc::new(Barrier::new(self.num_participants)),
                    };
                }
                closing_epoch
            }
            EpochManagerState::Closing { .. } => {
                unreachable!("We just modified `EpochManagerstate` to `Closed`")
            }
        }
    }
}
