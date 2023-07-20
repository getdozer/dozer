use std::borrow::Cow;
use std::time::SystemTime;

use crossbeam::channel::{Receiver, Select};
use dozer_types::epoch::{Epoch, RefOperation};
use dozer_types::{epoch::ExecutorOperation, log::debug};

use crate::errors::ExecutionError;

use super::{name::Name, InputPortState};

/// Common code for processor and sink nodes.
///
/// They both select from their input channels, and respond to "op", "commit", and terminate.
pub trait ReceiverLoop: Name {
    /// Returns input channels to this node. Will be called exactly once in [`receiver_loop`].
    fn receivers(&mut self) -> Vec<Receiver<ExecutorOperation>>;
    /// Returns the name of the receiver at `index`. Used for logging.
    fn receiver_name(&self, index: usize) -> Cow<str>;
    /// Responds to `op` from the receiver at `index`.
    fn on_op(&mut self, index: usize, op: RefOperation) -> Result<(), ExecutionError>;
    /// Responds to `commit` of `epoch`.
    fn on_commit(&mut self, epoch: &Epoch) -> Result<(), ExecutionError>;
    /// Responds to `terminate`.
    fn on_terminate(&mut self) -> Result<(), ExecutionError>;
    /// Responds to `SnapshottingDone`.
    fn on_snapshotting_done(&mut self, connection_name: String) -> Result<(), ExecutionError>;

    /// The loop implementation, calls [`on_op`], [`on_commit`] and [`on_terminate`] at appropriate times.
    fn receiver_loop(&mut self) -> Result<(), ExecutionError> {
        let receivers = self.receivers();
        debug_assert!(
            !receivers.is_empty(),
            "Processor or sink must have at least 1 incoming edge"
        );
        let mut port_states = vec![InputPortState::Open; receivers.len()];

        let mut commits_received: usize = 0;
        let mut common_epoch = Epoch::new(0, Default::default(), SystemTime::now());

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
                    assert_eq!(epoch.id, common_epoch.id);
                    commits_received += 1;
                    sel.remove(index);
                    // Commits from all inputs ports must have the same decision instant.
                    if commits_received > 1 {
                        assert_eq!(common_epoch.decision_instant, epoch.decision_instant);
                    }
                    common_epoch.decision_instant = epoch.decision_instant;
                    common_epoch.details.extend(epoch.details);

                    if commits_received == receivers.len() {
                        self.on_commit(&common_epoch)?;
                        common_epoch =
                            Epoch::new(common_epoch.id + 1, Default::default(), SystemTime::now());
                        commits_received = 0;
                        sel = init_select(&receivers);
                    }
                }
                ExecutorOperation::Terminate => {
                    port_states[index] = InputPortState::Terminated;
                    sel.remove(index);
                    debug!(
                        "[{}] Received Terminate request on port {}",
                        self.name(),
                        self.receiver_name(index)
                    );
                    if port_states.iter().all(|v| v == &InputPortState::Terminated) {
                        self.on_terminate()?;
                        debug!("[{}] Quit", self.name());
                        return Ok(());
                    }
                }
                ExecutorOperation::SnapshottingDone { connection_name } => {
                    self.on_snapshotting_done(connection_name)?;
                }
            }
        }
    }
}

fn init_select(receivers: &Vec<Receiver<ExecutorOperation>>) -> Select {
    let mut sel = Select::new();
    for r in receivers {
        sel.recv(r);
    }
    sel
}

#[cfg(test)]
mod tests {
    use std::mem::swap;

    use crossbeam::channel::{unbounded, Sender};
    use dozer_types::{
        node::{NodeHandle, OpIdentifier, SourceStates},
        types::{Field, Record},
    };

    use super::*;

    struct TestReceiverLoop {
        receivers: Vec<Receiver<ExecutorOperation>>,
        ops: Vec<(usize, RefOperation)>,
        commits: Vec<Epoch>,
        snapshotting_done: Vec<String>,
        num_terminations: usize,
    }

    impl Name for TestReceiverLoop {
        fn name(&self) -> Cow<str> {
            Cow::Borrowed("TestReceiverLoop")
        }
    }

    impl ReceiverLoop for TestReceiverLoop {
        fn receivers(&mut self) -> Vec<Receiver<ExecutorOperation>> {
            let mut result = vec![];
            swap(&mut self.receivers, &mut result);
            result
        }

        fn receiver_name(&self, index: usize) -> Cow<str> {
            Cow::Owned(format!("receiver_{index}"))
        }

        fn on_op(&mut self, index: usize, op: RefOperation) -> Result<(), ExecutionError> {
            self.ops.push((index, op));
            Ok(())
        }

        fn on_commit(&mut self, epoch: &Epoch) -> Result<(), ExecutionError> {
            self.commits.push(epoch.clone());
            Ok(())
        }

        fn on_terminate(&mut self) -> Result<(), ExecutionError> {
            self.num_terminations += 1;
            Ok(())
        }

        fn on_snapshotting_done(&mut self, connection_name: String) -> Result<(), ExecutionError> {
            self.snapshotting_done.push(connection_name);
            Ok(())
        }
    }

    impl TestReceiverLoop {
        fn new(num_receivers: usize) -> (TestReceiverLoop, Vec<Sender<ExecutorOperation>>) {
            let (senders, receivers) = (0..num_receivers).map(|_| unbounded()).unzip();
            (
                TestReceiverLoop {
                    receivers,
                    ops: vec![],
                    commits: vec![],
                    snapshotting_done: vec![],
                    num_terminations: 0,
                },
                senders,
            )
        }
    }

    #[test]
    fn receiver_loop_stops_on_terminate() {
        let (mut test_loop, senders) = TestReceiverLoop::new(2);
        senders[0].send(ExecutorOperation::Terminate).unwrap();
        senders[1].send(ExecutorOperation::Terminate).unwrap();
        test_loop.receiver_loop().unwrap();
        assert_eq!(test_loop.num_terminations, 1);
    }

    #[test]
    fn receiver_loop_forwards_snapshotting_done() {
        let connection_name = "test_connection".to_string();
        let (mut test_loop, senders) = TestReceiverLoop::new(2);
        senders[0]
            .send(ExecutorOperation::SnapshottingDone {
                connection_name: connection_name.clone(),
            })
            .unwrap();
        senders[0].send(ExecutorOperation::Terminate).unwrap();
        senders[1].send(ExecutorOperation::Terminate).unwrap();
        test_loop.receiver_loop().unwrap();
        assert_eq!(test_loop.snapshotting_done, vec![connection_name])
    }

    #[test]
    fn receiver_loop_forwards_op() {
        let (mut test_loop, senders) = TestReceiverLoop::new(2);
        let record = Record::new(vec![Field::Int(1)]);
        senders[0]
            .send(ExecutorOperation::Op {
                op: Operation::Insert {
                    new: record.clone(),
                },
            })
            .unwrap();
        senders[0].send(ExecutorOperation::Terminate).unwrap();
        senders[1].send(ExecutorOperation::Terminate).unwrap();
        test_loop.receiver_loop().unwrap();
        assert_eq!(test_loop.ops, vec![(0, Operation::Insert { new: record })]);
    }

    #[test]
    fn receiver_loop_merges_commit_epoch_and_increases_epoch_id() {
        let (mut test_loop, senders) = TestReceiverLoop::new(2);
        let mut details = SourceStates::default();
        details.insert(
            NodeHandle::new(None, "0".to_string()),
            OpIdentifier::new(0, 0),
        );
        let decision_instant = SystemTime::now();
        let mut epoch0 = Epoch::new(0, details, decision_instant);
        let mut details = SourceStates::default();
        details.insert(
            NodeHandle::new(None, "1".to_string()),
            OpIdentifier::new(0, 0),
        );
        let mut epoch1 = Epoch::new(0, details, decision_instant);
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
        epoch0.id = 1;
        epoch1.id = 1;
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
        test_loop.receiver_loop().unwrap();

        let mut details = SourceStates::new();
        details.extend(epoch0.details);
        details.extend(epoch1.details);
        assert_eq!(
            test_loop.commits,
            vec![
                Epoch::new(0, details.clone(), decision_instant),
                Epoch::new(1, details, decision_instant)
            ]
        );
    }

    #[test]
    #[should_panic]
    fn receiver_loop_panics_on_inconsistent_commit_epoch() {
        let (mut test_loop, senders) = TestReceiverLoop::new(2);
        let mut details = SourceStates::new();
        details.insert(
            NodeHandle::new(None, "0".to_string()),
            OpIdentifier::new(0, 0),
        );
        let decision_instant = SystemTime::now();
        let epoch0 = Epoch::new(0, details, decision_instant);
        let mut details = SourceStates::new();
        details.insert(
            NodeHandle::new(None, "1".to_string()),
            OpIdentifier::new(0, 0),
        );
        let epoch1 = Epoch::new(1, details, decision_instant);
        senders[0]
            .send(ExecutorOperation::Commit { epoch: epoch0 })
            .unwrap();
        senders[1]
            .send(ExecutorOperation::Commit { epoch: epoch1 })
            .unwrap();
        senders[0].send(ExecutorOperation::Terminate).unwrap();
        senders[1].send(ExecutorOperation::Terminate).unwrap();
        test_loop.receiver_loop().unwrap();
    }
}
