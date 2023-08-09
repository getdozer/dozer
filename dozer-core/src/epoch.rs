use dozer_types::parking_lot::Mutex;
use std::sync::{Arc, Barrier};
use std::thread::sleep;
use std::time::{Duration, SystemTime};

pub use dozer_types::epoch::Epoch;

#[derive(Debug)]
enum EpochManagerState {
    Closing {
        /// Current epoch id.
        epoch_id: u64,
        /// Whether we should tell the sources to terminate when this epoch closes.
        should_terminate: bool,
        /// Whether we should tell the sources to commit when this epoch closes.
        should_commit: bool,
        /// Sources wait on this barrier to synchronize an epoch close.
        barrier: Arc<Barrier>,
    },
    Closed {
        /// Whether sources should terminate.
        terminating: bool,
        /// Whether sources should commit.
        committing: bool,
        /// Closed epoch id.
        epoch_id: u64,
        /// Instant when the epoch was closed.
        instant: SystemTime,
        /// Number of sources that have confirmed the epoch close.
        num_source_confirmations: usize,
    },
}

#[derive(Debug)]
pub struct EpochManager {
    num_sources: usize,
    state: Mutex<EpochManagerState>,
}

#[derive(Debug, Clone, PartialEq)]
/// When all sources agrees on closing an epoch, the `EpochManager` will make decisions on how to close this epoch and return this struct.
pub struct ClosedEpoch {
    pub should_terminate: bool,
    pub epoch_id_if_should_commit: Option<u64>,
    pub decision_instant: SystemTime,
}

impl EpochManager {
    pub fn new(num_sources: usize) -> Self {
        debug_assert!(num_sources > 0);
        Self {
            num_sources,
            state: Mutex::new(EpochManagerState::Closing {
                epoch_id: 0,
                should_terminate: true,
                should_commit: false,
                barrier: Arc::new(Barrier::new(num_sources)),
            }),
        }
    }

    /// Waits for the epoch to close until all sources do so.
    ///
    /// Returns whether the participant should terminate, the epoch id if the source should commit, and the instant when the decision was made.
    ///
    /// # Arguments
    ///
    /// - `request_termination`: Whether the source wants to terminate. The `EpochManager` checks if all sources want to terminate and returns `true` if so.
    /// - `request_commit`: Whether the source wants to commit. The `EpochManager` checks if any source wants to commit and returns `Some` if so.
    pub fn wait_for_epoch_close(
        &self,
        request_termination: bool,
        request_commit: bool,
    ) -> ClosedEpoch {
        let barrier = loop {
            let mut state = self.state.lock();
            match &mut *state {
                EpochManagerState::Closing {
                    should_terminate,
                    should_commit,
                    barrier,
                    ..
                } => {
                    // If anyone doesn't want to terminate, we don't terminate.
                    *should_terminate = *should_terminate && request_termination;
                    // If anyone wants to commit, we commit.
                    *should_commit = *should_commit || request_commit;
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
            epoch_id,
            should_terminate,
            should_commit,
            ..
        } = &mut *state
        {
            *state = EpochManagerState::Closed {
                terminating: *should_terminate,
                committing: *should_commit,
                epoch_id: *epoch_id,
                instant: SystemTime::now(),
                num_source_confirmations: 0,
            };
        }

        match &mut *state {
            EpochManagerState::Closed {
                terminating,
                committing,
                epoch_id,
                instant,
                num_source_confirmations,
            } => {
                let result = ClosedEpoch {
                    should_terminate: *terminating,
                    epoch_id_if_should_commit: if *committing { Some(*epoch_id) } else { None },
                    decision_instant: *instant,
                };

                *num_source_confirmations += 1;
                if *num_source_confirmations == self.num_sources {
                    // This thread is the last one in this critical area.
                    *state = EpochManagerState::Closing {
                        epoch_id: if *committing {
                            *epoch_id + 1
                        } else {
                            *epoch_id
                        },
                        should_terminate: true,
                        should_commit: false,
                        barrier: Arc::new(Barrier::new(self.num_sources)),
                    };
                }

                result
            }
            EpochManagerState::Closing { .. } => {
                unreachable!("We just modified `EpochManagerState` to `Closed`")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread::scope;

    use super::*;

    const NUM_THREADS: u16 = 10;

    fn run_epoch_manager(
        termination_gen: &(impl Fn(u16) -> bool + Sync),
        commit_gen: &(impl Fn(u16) -> bool + Sync),
    ) -> ClosedEpoch {
        let epoch_manager = EpochManager::new(NUM_THREADS as usize);
        let epoch_manager = &epoch_manager;
        scope(|scope| {
            let handles = (0..NUM_THREADS)
                .map(|index| {
                    scope.spawn(move || {
                        epoch_manager
                            .wait_for_epoch_close(termination_gen(index), commit_gen(index))
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
        let ClosedEpoch {
            epoch_id_if_should_commit,
            ..
        } = run_epoch_manager(&|_| false, &|_| false);
        assert!(epoch_id_if_should_commit.is_none());

        // One source has new data, epoch should be closed.
        let ClosedEpoch {
            epoch_id_if_should_commit,
            ..
        } = run_epoch_manager(&|_| false, &|index| index == 0);
        assert_eq!(epoch_id_if_should_commit.unwrap(), 0);

        // All but one source requests termination, should not terminate.
        let ClosedEpoch {
            should_terminate, ..
        } = run_epoch_manager(&|index| index != 0, &|_| false);
        assert!(!should_terminate);

        // All sources requests termination, should terminate.
        let ClosedEpoch {
            should_terminate, ..
        } = run_epoch_manager(&|_| true, &|_| false);
        assert!(should_terminate);
    }
}
