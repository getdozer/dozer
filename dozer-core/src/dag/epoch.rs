use crate::dag::node::NodeHandle;
use dozer_types::parking_lot::Mutex;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Barrier};
use std::thread::sleep;
use std::time::Duration;

#[derive(Clone, Debug, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OpIdentifier {
    pub txid: u64,
    pub seq_in_tx: u64,
}

impl OpIdentifier {
    pub fn new(txid: u64, seq_in_tx: u64) -> Self {
        Self { txid, seq_in_tx }
    }
}

#[derive(Clone, Debug, Default)]
pub struct PipelineCheckpoint(pub HashMap<NodeHandle, Option<OpIdentifier>>);

impl PartialEq for PipelineCheckpoint {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for PipelineCheckpoint {}

impl PartialOrd for PipelineCheckpoint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PipelineCheckpoint {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.0.len() != other.0.len() {
            panic!("Cannot compare two checkpoints with different number of sources");
        }

        let mut result = Ordering::Equal;
        for (key, value) in &self.0 {
            match value.cmp(
                other
                    .0
                    .get(key)
                    .expect("Cannot compare two checkpoints with different sources"),
            ) {
                Ordering::Equal => {}
                Ordering::Less => {
                    if result == Ordering::Greater {
                        panic!("Cannot compare two checkpoints with inconsistent source ordering");
                    }
                    result = Ordering::Less;
                }
                Ordering::Greater => {
                    if result == Ordering::Less {
                        panic!("Cannot compare two checkpoints with inconsistent source ordering");
                    }
                    result = Ordering::Greater;
                }
            }
        }
        result
    }
}

impl PipelineCheckpoint {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Epoch {
    pub id: u64,
    pub details: PipelineCheckpoint,
}

impl Epoch {
    pub fn new(id: u64, details: PipelineCheckpoint) -> Self {
        Self { id, details }
    }

    pub fn from(id: u64, node_handle: NodeHandle, txid: u64, seq_in_tx: u64) -> Self {
        Self {
            id,
            details: PipelineCheckpoint(
                [(node_handle, Some(OpIdentifier::new(txid, seq_in_tx)))]
                    .into_iter()
                    .collect(),
            ),
        }
    }
}

impl Display for Epoch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let details_str = self
            .details
            .0
            .iter()
            .map(|e| {
                if let Some(op_id) = e.1 {
                    format!("{} -> {}:{}", e.0, op_id.txid, op_id.seq_in_tx)
                } else {
                    format!("{} -> None", e.0)
                }
            })
            .fold(String::new(), |a, b| a + ", " + b.as_str());
        f.write_str(format!("epoch: {}, details: {}", self.id, details_str).as_str())
    }
}

#[derive(Debug)]
enum EpochManagerState {
    Closing {
        /// Last epoch, if any.
        last_epoch: Option<Epoch>,
        /// Current epoch. Details of this epoch get inserted as participants wait for the epoch to close.
        epoch: Epoch,
        /// Whether we should tell the participants to terminate when this epoch closes.
        should_terminate: bool,
        /// Participants wait on this barrier to synchronize an epoch close.
        barrier: Arc<Barrier>,
    },
    Closed {
        /// Whether participants should terminate.
        terminating: bool,
        /// Whether participants should commit.
        committing: bool,
        /// Up-to-date details of all participants. Participants will commit with details from this epoch if they're committing.
        epoch: Epoch,
        /// Number of participants that have confirmed the epoch close.
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
                last_epoch: None,
                epoch: Epoch::new(0, PipelineCheckpoint::default()),
                should_terminate: true,
                barrier: Arc::new(Barrier::new(num_participants)),
            }),
        }
    }

    /// Waits for the epoch to close until all participants do so.
    ///
    /// Returns whether the participant should terminate and the epoch details if the participant should commit.
    ///
    /// # Arguments
    ///
    /// - `participant`: The participant identifier.
    /// - `txn_id_and_seq_number`: The transaction ID and sequence number of the last operation processed by the participant in this epoch.
    /// - `request_termination`: Whether the participant wants to terminate. The `EpochManager` checks if all participants want to terminate and returns `true` if so.
    pub fn wait_for_epoch_close(
        &self,
        participant: NodeHandle,
        txn_id_and_seq_number: Option<(u64, u64)>,
        request_termination: bool,
    ) -> (bool, Option<Epoch>) {
        let barrier = loop {
            let mut state = self.state.lock();
            match &mut *state {
                EpochManagerState::Closing {
                    epoch,
                    should_terminate,
                    barrier,
                    ..
                } => {
                    epoch.details.0.insert(
                        participant,
                        txn_id_and_seq_number
                            .map(|(txid, seq_in_tx)| OpIdentifier::new(txid, seq_in_tx)),
                    );
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
            last_epoch,
            epoch,
            should_terminate,
            ..
        } = &mut *state
        {
            // This thread is the first one in this critical area.
            debug_assert!(epoch.details.0.len() == self.num_participants);

            let terminating = *should_terminate;

            let committing = epoch.details.0.values().any(|value| value.is_some());

            let mut up_to_date_details = HashMap::new();
            for (key, mut value) in epoch.details.0.drain() {
                if value.is_none() {
                    // This participant doesn't have new op in this epoch.
                    if let Some(last_epoch) = last_epoch.as_mut() {
                        // Copy the last epoch's details.
                        value = last_epoch
                            .details
                            .0
                            .remove(&key)
                            .expect("All epoch should contain all participant details");
                    }
                }
                up_to_date_details.insert(key, value);
            }

            *state = EpochManagerState::Closed {
                terminating,
                committing,
                epoch: Epoch {
                    id: epoch.id,
                    details: PipelineCheckpoint(up_to_date_details),
                },
                num_participant_confirmations: 0,
            };
        }

        match &mut *state {
            EpochManagerState::Closed {
                terminating,
                committing,
                epoch,
                num_participant_confirmations,
            } => {
                let result = if *committing {
                    (*terminating, Some(epoch.clone()))
                } else {
                    (*terminating, None)
                };

                *num_participant_confirmations += 1;
                if *num_participant_confirmations == self.num_participants {
                    // This thread is the last one in this critical area.
                    let next_epoch_id = if *committing { epoch.id + 1 } else { epoch.id };
                    *state = EpochManagerState::Closing {
                        last_epoch: Some(epoch.clone()),
                        epoch: Epoch::new(next_epoch_id, Default::default()),
                        should_terminate: true,
                        barrier: Arc::new(Barrier::new(self.num_participants)),
                    };
                }

                result
            }
            EpochManagerState::Closing { .. } => {
                unreachable!("We just modified `EpochManagerstate` to `Closed`")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread::scope;

    use super::*;

    fn new_node_handle(index: u16) -> NodeHandle {
        NodeHandle::new(Some(index), format!("node{}", index))
    }

    fn new_checkpoint(node_checkpoints: Vec<(u16, Option<u64>)>) -> PipelineCheckpoint {
        let mut checkpoint = PipelineCheckpoint::default();
        for (node, seq) in node_checkpoints {
            checkpoint.0.insert(
                new_node_handle(node),
                seq.map(|seq| OpIdentifier::new(0, seq)),
            );
        }
        checkpoint
    }

    #[test]
    fn test_pipeline_checkpoint_ord() {
        assert_eq!(new_checkpoint(vec![]), new_checkpoint(vec![]));
        assert_eq!(
            new_checkpoint(vec![(1, Some(1))]),
            new_checkpoint(vec![(1, Some(1))])
        );
        assert!(new_checkpoint(vec![(1, None)]) < new_checkpoint(vec![(1, Some(0))]));
        assert!(new_checkpoint(vec![(1, Some(0))]) > new_checkpoint(vec![(1, None)]));
        assert_eq!(
            new_checkpoint(vec![(1, Some(1)), (2, Some(2))]),
            new_checkpoint(vec![(1, Some(1)), (2, Some(2))])
        );
        assert!(
            new_checkpoint(vec![(1, Some(1)), (2, Some(2))])
                < new_checkpoint(vec![(1, Some(1)), (2, Some(3))])
        );
        assert!(
            new_checkpoint(vec![(1, Some(1)), (2, Some(2))])
                > new_checkpoint(vec![(1, Some(1)), (2, Some(1))])
        );
    }

    #[test]
    #[should_panic]
    fn compare_pipeline_checkpoint_with_different_number_of_nodes_should_panic() {
        let checkpoint1 = new_checkpoint(vec![(1, Some(1)), (2, Some(2))]);
        let checkpoint2 = new_checkpoint(vec![(1, Some(1))]);
        let _ = checkpoint1.cmp(&checkpoint2);
    }

    #[test]
    #[should_panic]
    fn compare_pipeline_checkpoint_with_different_nodes_should_panic() {
        let checkpoint1 = new_checkpoint(vec![(1, Some(1))]);
        let checkpoint2 = new_checkpoint(vec![(2, Some(1))]);
        let _ = checkpoint1.cmp(&checkpoint2);
    }

    #[test]
    #[should_panic]
    fn compare_pipeline_checkpoint_with_undefined_order_should_panic_1() {
        let checkpoint1 = new_checkpoint(vec![(1, Some(1)), (2, None)]);
        let checkpoint2 = new_checkpoint(vec![(1, None), (2, Some(1))]);
        let _ = checkpoint1.cmp(&checkpoint2);
    }

    #[test]
    #[should_panic]
    fn compare_pipeline_checkpoint_with_undefined_order_should_panic_2() {
        let checkpoint1 = new_checkpoint(vec![(1, Some(1)), (2, None)]);
        let checkpoint2 = new_checkpoint(vec![(1, None), (2, Some(1))]);
        let _ = checkpoint2.cmp(&checkpoint1);
    }

    const NUM_THREADS: u16 = 10;

    fn run_epoch_manager(
        op_id_gen: &(impl Fn(u16) -> Option<(u64, u64)> + Sync),
        termination_gen: &(impl Fn(u16) -> bool + Sync),
    ) -> (bool, Option<Epoch>) {
        let epoch_manager = EpochManager::new(NUM_THREADS as usize);
        let epoch_manager = &epoch_manager;
        scope(|scope| {
            let handles = (0..NUM_THREADS)
                .map(|index| {
                    scope.spawn(move || {
                        epoch_manager.wait_for_epoch_close(
                            new_node_handle(index),
                            op_id_gen(index),
                            termination_gen(index),
                        )
                    })
                })
                .collect::<Vec<_>>();
            let results = handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>();
            for result in &results {
                assert_eq!(result, results.first().unwrap());
            }
            results.into_iter().next().unwrap()
        })
    }

    #[test]
    fn test_epoch_manager() {
        // All sources have no new data, epoch should not be closed.
        let (_, epoch) = run_epoch_manager(&|_| None, &|_| false);
        assert!(epoch.is_none());

        // One source has new data, epoch should be closed.
        let (_, epoch) = run_epoch_manager(
            &|index| if index == 0 { Some((0, 0)) } else { None },
            &|_| false,
        );
        let epoch = epoch.unwrap();
        assert_eq!(
            epoch.details.0.get(&new_node_handle(0)).unwrap().unwrap(),
            OpIdentifier::new(0, 0)
        );

        // All but one source requests termination, should not terminate.
        let (terminating, _) = run_epoch_manager(&|_| None, &|index| index != 0);
        assert!(!terminating);

        // All sources requests termination, should terminate.
        let (terminating, _) = run_epoch_manager(&|_| None, &|_| true);
        assert!(terminating);
    }
}
