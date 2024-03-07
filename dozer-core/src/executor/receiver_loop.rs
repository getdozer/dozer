use std::borrow::Cow;

use crossbeam::channel::{Receiver, Select};
use dozer_types::{log::debug, node::OpIdentifier, types::TableOperation};

use crate::{epoch::Epoch, errors::ExecutionError, executor_operation::ExecutorOperation};

use super::name::Name;

/// Common code for processor and sink nodes.
///
/// They both select from their input channels, and respond to "op", "commit", and terminate.
pub trait ReceiverLoop: Name {
    /// Returns the epoch id that this node was constructed for.
    fn initial_epoch_id(&self) -> u64;
    /// Returns input channels to this node. Will be called exactly once in [`receiver_loop`].
    fn receivers(&mut self) -> Vec<Receiver<ExecutorOperation>>;
    /// Returns the name of the receiver at `index`. Used for logging.
    fn receiver_name(&self, index: usize) -> Cow<str>;
    /// Responds to `op` from the receiver at `index`.
    fn on_op(&mut self, index: usize, op: TableOperation) -> Result<(), ExecutionError>;
    /// Responds to `commit` of `epoch`.
    fn on_commit(&mut self, epoch: Epoch) -> Result<(), ExecutionError>;
    /// Responds to `terminate`.
    fn on_terminate(&mut self) -> Result<(), ExecutionError>;
    /// Responds to `SnapshottingStarted`.
    fn on_snapshotting_started(&mut self, connection_name: String) -> Result<(), ExecutionError>;
    /// Responds to `SnapshottingDone`.
    fn on_snapshotting_done(
        &mut self,
        connection_name: String,
        id: Option<OpIdentifier>,
    ) -> Result<(), ExecutionError>;

    /// The loop implementation, calls [`on_op`], [`on_commit`] and [`on_terminate`] at appropriate times.
    fn receiver_loop(mut self, initial_epoch_id: u64) -> Result<(), ExecutionError>
    where
        Self: Sized,
    {
        let receivers = self.receivers();
        debug_assert!(
            !receivers.is_empty(),
            "Processor or sink must have at least 1 incoming edge"
        );
        let mut is_terminated = vec![false; receivers.len()];

        let mut commits_received: usize = 0;
        let mut epoch_id = initial_epoch_id;

        let mut sel = init_select(&receivers);
        loop {
            let index = sel.ready();
            let op = receivers[index]
                .recv()
                .map_err(|_| ExecutionError::CannotReceiveFromChannel)?;

            match op {
                ExecutorOperation::Op { op } => {
                    self.on_op(index, op)?;
                }
                ExecutorOperation::Commit { epoch } => {
                    assert_eq!(epoch.common_info.id, epoch_id);
                    commits_received += 1;
                    sel.remove(index);

                    if commits_received == receivers.len() {
                        self.on_commit(epoch)?;
                        epoch_id += 1;
                        commits_received = 0;
                        sel = init_select(&receivers);
                    }
                }
                ExecutorOperation::Terminate => {
                    is_terminated[index] = true;
                    sel.remove(index);
                    debug!(
                        "[{}] Received Terminate request from {}",
                        self.name(),
                        self.receiver_name(index)
                    );
                    if is_terminated.iter().all(|value| *value) {
                        self.on_terminate()?;
                        debug!("[{}] Quit", self.name());
                        return Ok(());
                    }
                }
                ExecutorOperation::SnapshottingStarted { connection_name } => {
                    self.on_snapshotting_started(connection_name)?;
                }
                ExecutorOperation::SnapshottingDone {
                    connection_name,
                    id,
                } => {
                    self.on_snapshotting_done(connection_name, id)?;
                }
            }
        }
    }
}

pub(crate) fn init_select(receivers: &Vec<Receiver<ExecutorOperation>>) -> Select {
    let mut sel = Select::new();
    for r in receivers {
        sel.recv(r);
    }
    sel
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, mem::swap, rc::Rc, sync::Arc, time::SystemTime};

    use crossbeam::channel::{unbounded, Sender};
    use dozer_types::{
        node::{NodeHandle, SourceState, SourceStates},
        types::{Field, Operation, Record},
    };

    use crate::DEFAULT_PORT_HANDLE;

    use super::*;

    #[derive(Clone)]
    struct TestReceiverLoopState {
        ops: Vec<(usize, TableOperation)>,
        commits: Vec<Epoch>,
        snapshotting_started: Vec<String>,
        snapshotting_done: Vec<(String, Option<OpIdentifier>)>,
        num_terminations: usize,
    }

    struct TestReceiverLoop {
        receivers: Vec<Receiver<ExecutorOperation>>,
        state: Rc<RefCell<TestReceiverLoopState>>,
    }

    impl Name for TestReceiverLoop {
        fn name(&self) -> Cow<str> {
            Cow::Borrowed("TestReceiverLoop")
        }
    }

    impl ReceiverLoop for TestReceiverLoop {
        fn initial_epoch_id(&self) -> u64 {
            0
        }

        fn receivers(&mut self) -> Vec<Receiver<ExecutorOperation>> {
            let mut result = vec![];
            swap(&mut self.receivers, &mut result);
            result
        }

        fn receiver_name(&self, index: usize) -> Cow<str> {
            Cow::Owned(format!("receiver_{index}"))
        }

        fn on_op(&mut self, index: usize, op: TableOperation) -> Result<(), ExecutionError> {
            self.state.borrow_mut().ops.push((index, op));
            Ok(())
        }

        fn on_commit(&mut self, epoch: Epoch) -> Result<(), ExecutionError> {
            self.state.borrow_mut().commits.push(epoch);
            Ok(())
        }

        fn on_terminate(&mut self) -> Result<(), ExecutionError> {
            self.state.borrow_mut().num_terminations += 1;
            Ok(())
        }

        fn on_snapshotting_started(
            &mut self,
            connection_name: String,
        ) -> Result<(), ExecutionError> {
            self.state
                .borrow_mut()
                .snapshotting_started
                .push(connection_name);
            Ok(())
        }

        fn on_snapshotting_done(
            &mut self,
            connection_name: String,
            state: Option<OpIdentifier>,
        ) -> Result<(), ExecutionError> {
            self.state
                .borrow_mut()
                .snapshotting_done
                .push((connection_name, state));
            Ok(())
        }
    }

    impl TestReceiverLoop {
        fn new(
            num_receivers: usize,
        ) -> (
            TestReceiverLoop,
            Vec<Sender<ExecutorOperation>>,
            Rc<RefCell<TestReceiverLoopState>>,
        ) {
            let (senders, receivers) = (0..num_receivers).map(|_| unbounded()).unzip();
            let state = Rc::new(RefCell::new(TestReceiverLoopState {
                ops: vec![],
                commits: vec![],
                snapshotting_started: vec![],
                snapshotting_done: vec![],
                num_terminations: 0,
            }));
            (
                TestReceiverLoop {
                    receivers,
                    state: state.clone(),
                },
                senders,
                state,
            )
        }
    }

    #[test]
    fn receiver_loop_stops_on_terminate() {
        let (test_loop, senders, state) = TestReceiverLoop::new(2);
        let test_loop = Box::new(test_loop);
        senders[0].send(ExecutorOperation::Terminate).unwrap();
        senders[1].send(ExecutorOperation::Terminate).unwrap();
        test_loop.receiver_loop(0).unwrap();
        assert_eq!(state.borrow().num_terminations, 1);
    }

    #[test]
    fn receiver_loop_forwards_snapshotting_done() {
        let connection_name = "test_connection".to_string();
        let (test_loop, senders, state) = TestReceiverLoop::new(2);
        senders[0]
            .send(ExecutorOperation::SnapshottingDone {
                connection_name: connection_name.clone(),
                id: None,
            })
            .unwrap();
        senders[0].send(ExecutorOperation::Terminate).unwrap();
        senders[1].send(ExecutorOperation::Terminate).unwrap();
        test_loop.receiver_loop(0).unwrap();
        let snapshotting_done = state.borrow().snapshotting_done.clone();
        assert_eq!(snapshotting_done, vec![(connection_name, None)])
    }

    #[test]
    fn receiver_loop_forwards_op() {
        let (test_loop, senders, state) = TestReceiverLoop::new(2);
        let record = Record::new(vec![Field::Int(1)]);
        senders[0]
            .send(ExecutorOperation::Op {
                op: TableOperation::without_id(
                    Operation::Insert {
                        new: record.clone(),
                    },
                    DEFAULT_PORT_HANDLE,
                ),
            })
            .unwrap();
        senders[0].send(ExecutorOperation::Terminate).unwrap();
        senders[1].send(ExecutorOperation::Terminate).unwrap();
        test_loop.receiver_loop(0).unwrap();
        assert_eq!(
            state.borrow().ops,
            vec![(
                0,
                TableOperation::without_id(Operation::Insert { new: record }, DEFAULT_PORT_HANDLE,)
            )]
        );
    }

    #[test]
    fn receiver_loop_increases_epoch_id() {
        let (test_loop, senders, state) = TestReceiverLoop::new(2);
        let mut source_states = SourceStates::default();
        source_states.insert(
            NodeHandle::new(None, "0".to_string()),
            SourceState::NotStarted,
        );
        source_states.insert(
            NodeHandle::new(None, "1".to_string()),
            SourceState::NotStarted,
        );
        let source_states = Arc::new(source_states);
        let decision_instant = SystemTime::now();
        let mut epoch0 = Epoch::new(0, source_states.clone(), decision_instant);
        let mut epoch1 = Epoch::new(0, source_states, decision_instant);
        senders[0]
            .send(ExecutorOperation::Commit {
                epoch: epoch0.clone(),
            })
            .unwrap();
        senders[1]
            .send(ExecutorOperation::Commit {
                epoch: epoch1.clone(),
            })
            .unwrap();
        epoch0.common_info.id = 1;
        epoch1.common_info.id = 1;
        senders[0]
            .send(ExecutorOperation::Commit {
                epoch: epoch0.clone(),
            })
            .unwrap();
        senders[1]
            .send(ExecutorOperation::Commit {
                epoch: epoch1.clone(),
            })
            .unwrap();
        senders[0].send(ExecutorOperation::Terminate).unwrap();
        senders[1].send(ExecutorOperation::Terminate).unwrap();
        test_loop.receiver_loop(0).unwrap();

        let state = state.borrow();
        assert_eq!(state.commits[0].common_info.id, 0);
        assert_eq!(state.commits[0].decision_instant, decision_instant);
        assert_eq!(state.commits[1].common_info.id, 1);
        assert_eq!(state.commits[1].decision_instant, decision_instant);
    }

    #[test]
    #[should_panic]
    fn receiver_loop_panics_on_inconsistent_commit_epoch() {
        let (test_loop, senders, _) = TestReceiverLoop::new(2);
        let mut source_states = SourceStates::new();
        source_states.insert(
            NodeHandle::new(None, "0".to_string()),
            SourceState::NotStarted,
        );
        source_states.insert(
            NodeHandle::new(None, "1".to_string()),
            SourceState::NotStarted,
        );
        let source_states = Arc::new(source_states);
        let decision_instant = SystemTime::now();
        let epoch0 = Epoch::new(0, source_states.clone(), decision_instant);
        let epoch1 = Epoch::new(1, source_states, decision_instant);
        senders[0]
            .send(ExecutorOperation::Commit { epoch: epoch0 })
            .unwrap();
        senders[1]
            .send(ExecutorOperation::Commit { epoch: epoch1 })
            .unwrap();
        senders[0].send(ExecutorOperation::Terminate).unwrap();
        senders[1].send(ExecutorOperation::Terminate).unwrap();
        test_loop.receiver_loop(0).unwrap();
    }
}
