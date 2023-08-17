use dozer_types::parking_lot::Mutex;
use std::ops::DerefMut;
use std::sync::{Arc, Barrier};
use std::thread::sleep;
use std::time::{Duration, SystemTime};

use crate::checkpoint::{CheckpointFactory, CheckpointWriter};
use crate::processor_record::ProcessorRecordStore;

use super::EpochCommonInfo;

#[derive(Debug)]
struct EpochManagerState {
    kind: EpochManagerStateKind,
    /// Initialized to 0.
    next_record_index_to_persist: usize,
    /// The instant when epoch manager decided to persist the last epoch. Initialized to the epoch manager's start time.
    last_persisted_epoch_decision_instant: SystemTime,
}

#[derive(Debug)]
enum EpochManagerStateKind {
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
        /// - `None`: Should not commit.
        /// - `Some(false)`: Should commit but should not persist.
        /// - `Some(true)`: Should commit and persist records from `index`.
        should_persist_if_committing: Option<bool>,
        /// Closed epoch id.
        epoch_id: u64,
        /// Instant when the epoch was closed.
        instant: SystemTime,
        /// Number of sources that have confirmed the epoch close.
        num_source_confirmations: usize,
    },
}

impl EpochManagerStateKind {
    fn new_closing(epoch_id: u64, num_sources: usize) -> EpochManagerStateKind {
        EpochManagerStateKind::Closing {
            epoch_id,
            should_terminate: true,
            should_commit: false,
            barrier: Arc::new(Barrier::new(num_sources)),
        }
    }
}

#[derive(Debug)]
pub struct EpochManagerOptions {
    pub max_num_records_before_persist: usize,
    pub max_interval_before_persist_in_seconds: u64,
}

impl Default for EpochManagerOptions {
    fn default() -> Self {
        Self {
            max_num_records_before_persist: 100_000,
            max_interval_before_persist_in_seconds: 60,
        }
    }
}

#[derive(Debug)]
pub struct EpochManager {
    num_sources: usize,
    checkpoint_factory: Arc<CheckpointFactory>,
    options: EpochManagerOptions,
    state: Mutex<EpochManagerState>,
}

#[derive(Debug, Clone)]
/// When all sources agrees on closing an epoch, the `EpochManager` will make decisions on how to close this epoch and return this struct.
pub struct ClosedEpoch {
    pub should_terminate: bool,
    /// `Some` if the epoch should be committed.
    pub common_info: Option<EpochCommonInfo>,
    pub decision_instant: SystemTime,
}

impl EpochManager {
    pub fn new(
        num_sources: usize,
        checkpoint_factory: Arc<CheckpointFactory>,
        options: EpochManagerOptions,
    ) -> Self {
        debug_assert!(num_sources > 0);
        Self {
            num_sources,
            checkpoint_factory,
            options,
            state: Mutex::new(EpochManagerState {
                kind: EpochManagerStateKind::new_closing(0, num_sources),
                next_record_index_to_persist: 0,
                last_persisted_epoch_decision_instant: SystemTime::now(),
            }),
        }
    }

    pub fn record_store(&self) -> &Arc<ProcessorRecordStore> {
        self.checkpoint_factory.record_store()
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
            match &mut state.kind {
                EpochManagerStateKind::Closing {
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
                EpochManagerStateKind::Closed { .. } => {
                    // This thread wants to close a new epoch while some other thread hasn't got confirmation of last epoch closing.
                    // Just release the lock and put this thread to sleep.
                    drop(state);
                    sleep(Duration::from_millis(1));
                }
            }
        };

        barrier.wait();

        let mut state = self.state.lock();
        let state = state.deref_mut();
        if let EpochManagerStateKind::Closing {
            epoch_id,
            should_terminate,
            should_commit,
            ..
        } = &mut state.kind
        {
            let instant = SystemTime::now();
            let should_persist_if_committing = if *should_commit {
                let num_records = self.record_store().num_records();
                let should_persist = if num_records - state.next_record_index_to_persist
                    >= self.options.max_num_records_before_persist
                    || instant
                        .duration_since(state.last_persisted_epoch_decision_instant)
                        .unwrap_or(Duration::from_secs(0))
                        >= Duration::from_secs(self.options.max_interval_before_persist_in_seconds)
                {
                    state.next_record_index_to_persist = num_records;
                    state.last_persisted_epoch_decision_instant = instant;
                    true
                } else {
                    false
                };

                Some(should_persist)
            } else {
                None
            };

            state.kind = EpochManagerStateKind::Closed {
                terminating: *should_terminate,
                should_persist_if_committing,
                epoch_id: *epoch_id,
                instant,
                num_source_confirmations: 0,
            };
        }

        match &mut state.kind {
            EpochManagerStateKind::Closed {
                terminating,
                should_persist_if_committing,
                epoch_id,
                instant,
                num_source_confirmations,
            } => {
                let common_info = should_persist_if_committing.map(|should_persist| {
                    let checkpoint_writer = if should_persist {
                        Some(Arc::new(CheckpointWriter::new(
                            self.checkpoint_factory.clone(),
                            *epoch_id,
                        )))
                    } else {
                        None
                    };
                    EpochCommonInfo {
                        id: *epoch_id,
                        checkpoint_writer,
                    }
                });

                let result = ClosedEpoch {
                    should_terminate: *terminating,
                    common_info,
                    decision_instant: *instant,
                };

                *num_source_confirmations += 1;
                if *num_source_confirmations == self.num_sources {
                    // This thread is the last one in this critical area.
                    state.kind = EpochManagerStateKind::new_closing(
                        if should_persist_if_committing.is_some() {
                            *epoch_id + 1
                        } else {
                            *epoch_id
                        },
                        self.num_sources,
                    );
                }

                result
            }
            EpochManagerStateKind::Closing { .. } => {
                unreachable!("We just modified `EpochManagerState` to `Closed`")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread::scope;

    use dozer_log::tokio;
    use tempdir::TempDir;

    use crate::checkpoint::create_checkpoint_factory_for_test;

    use super::*;

    const NUM_THREADS: u16 = 10;

    async fn create_epoch_manager(
        num_sources: usize,
        options: EpochManagerOptions,
    ) -> (TempDir, EpochManager) {
        let (temp_dir, checkpoint_factory, _) = create_checkpoint_factory_for_test(&[]).await;

        let epoch_manager = EpochManager::new(num_sources, checkpoint_factory, options);

        (temp_dir, epoch_manager)
    }

    fn run_epoch_manager(
        epoch_manager: &EpochManager,
        termination_gen: &(impl Fn(u16) -> bool + Sync),
        commit_gen: &(impl Fn(u16) -> bool + Sync),
    ) -> ClosedEpoch {
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

            let first = results.first().unwrap();
            for result in &results {
                assert_eq!(result.should_terminate, first.should_terminate);
                assert_eq!(result.common_info.is_some(), first.common_info.is_some());
                if let Some(common_info) = &result.common_info {
                    assert_eq!(common_info.id, first.common_info.as_ref().unwrap().id);
                }
                assert_eq!(result.decision_instant, first.decision_instant);
            }
            results.into_iter().next().unwrap()
        })
    }

    #[tokio::test]
    async fn test_epoch_manager() {
        let (_temp_dir, epoch_manager) =
            create_epoch_manager(NUM_THREADS as usize, Default::default()).await;

        // All sources have no new data, epoch should not be closed.
        let ClosedEpoch { common_info, .. } =
            run_epoch_manager(&epoch_manager, &|_| false, &|_| false);
        assert!(common_info.is_none());

        // One source has new data, epoch should be closed.
        let ClosedEpoch { common_info, .. } =
            run_epoch_manager(&epoch_manager, &|_| false, &|index| index == 0);
        assert_eq!(common_info.unwrap().id, 0);

        // All but one source requests termination, should not terminate.
        let ClosedEpoch {
            should_terminate, ..
        } = run_epoch_manager(&epoch_manager, &|index| index != 0, &|_| false);
        assert!(!should_terminate);

        // All sources requests termination, should terminate.
        let ClosedEpoch {
            should_terminate, ..
        } = run_epoch_manager(&epoch_manager, &|_| true, &|_| false);
        assert!(should_terminate);
    }

    #[tokio::test]
    async fn test_epoch_manager_persist_message() {
        let (_temp_dir, epoch_manager) = create_epoch_manager(
            1,
            EpochManagerOptions {
                max_num_records_before_persist: 1,
                max_interval_before_persist_in_seconds: 1,
            },
        )
        .await;

        // Epoch manager must be used from non-tokio threads.
        std::thread::spawn(move || {
            // No record, no persist.
            let epoch = epoch_manager.wait_for_epoch_close(false, true);
            assert!(epoch.common_info.unwrap().checkpoint_writer.is_none());

            // One record, persist.
            epoch_manager.record_store().create_ref(&[]).unwrap();
            let epoch = epoch_manager.wait_for_epoch_close(false, true);
            assert!(epoch.common_info.unwrap().checkpoint_writer.is_some());

            // Time passes, persist.
            std::thread::sleep(Duration::from_secs(1));
            let epoch = epoch_manager.wait_for_epoch_close(false, true);
            assert!(epoch.common_info.unwrap().checkpoint_writer.is_some());
        })
        .join()
        .unwrap();
    }
}
