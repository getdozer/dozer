use std::borrow::Cow;

use crossbeam::channel::Receiver;
use dozer_types::log::debug;
use dozer_types::{internal_err, types::Operation};

use crate::{
    epoch::Epoch,
    errors::ExecutionError::{self, InternalError},
    executor_utils::init_select,
};

use super::{name::Name, ExecutorOperation, InputPortState};

#[derive(Debug, PartialEq)]
enum MappedExecutorOperation {
    Data { op: Operation },
    Commit { epoch: Epoch },
    Terminate,
}

fn map_executor_operation(op: ExecutorOperation) -> MappedExecutorOperation {
    match op {
        ExecutorOperation::Delete { old } => MappedExecutorOperation::Data {
            op: Operation::Delete { old },
        },
        ExecutorOperation::Insert { new } => MappedExecutorOperation::Data {
            op: Operation::Insert { new },
        },
        ExecutorOperation::Update { old, new } => MappedExecutorOperation::Data {
            op: Operation::Update { old, new },
        },
        ExecutorOperation::Commit { epoch } => MappedExecutorOperation::Commit { epoch },
        ExecutorOperation::Terminate => MappedExecutorOperation::Terminate,
    }
}

/// Common code for processor and sink nodes.
///
/// They both select from their input channels, and respond to "op", "commit", and terminate.
pub trait ReceiverLoop: Name {
    /// Returns input channels to this node. Will be called exactly once in [`receiver_loop`].
    fn receivers(&mut self) -> Vec<Receiver<ExecutorOperation>>;
    /// Returns the name of the receiver at `index`. Used for logging.
    fn receiver_name(&self, index: usize) -> Cow<str>;
    /// Responds to `op` from the receiver at `index`.
    fn on_op(&mut self, index: usize, op: Operation) -> Result<(), ExecutionError>;
    /// Responds to `commit` of `epoch`.
    fn on_commit(&mut self, epoch: &Epoch) -> Result<(), ExecutionError>;
    /// Responds to `terminate`.
    fn on_terminate(&mut self) -> Result<(), ExecutionError>;

    /// The loop implementation, calls [`on_op`], [`on_commit`] and [`on_terminate`] at appropriate times.
    fn receiver_loop(&mut self) -> Result<(), ExecutionError> {
        let receivers = self.receivers();
        debug_assert!(
            !receivers.is_empty(),
            "Processor or sink must have at least 1 incoming edge"
        );
        let mut port_states = vec![InputPortState::Open; receivers.len()];

        let mut commits_received: usize = 0;
        let mut common_epoch = Epoch::new(0, Default::default());

        let mut sel = init_select(&receivers);
        loop {
            let index = sel.ready();
            match internal_err!(receivers[index].recv().map(map_executor_operation))? {
                MappedExecutorOperation::Data { op } => {
                    self.on_op(index, op)?;
                }
                MappedExecutorOperation::Commit { epoch } => {
                    assert_eq!(epoch.id, common_epoch.id);
                    commits_received += 1;
                    sel.remove(index);
                    common_epoch.details.extend(epoch.details);

                    if commits_received == receivers.len() {
                        self.on_commit(&common_epoch)?;
                        common_epoch = Epoch::new(common_epoch.id + 1, Default::default());
                        commits_received = 0;
                        sel = init_select(&receivers);
                    }
                }
                MappedExecutorOperation::Terminate => {
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
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::mem::swap;

    use crossbeam::channel::{unbounded, Sender};
    use dozer_types::types::{Field, Record};

    use crate::{
        epoch::{OpIdentifier, SourceStates},
        node::NodeHandle,
    };

    use super::*;

    #[test]
    fn test_map_executor_operation() {
        let old = Record::new(None, vec![Field::Int(1)], None);
        let new = Record::new(None, vec![Field::Int(2)], None);
        let epoch = Epoch::new(1, Default::default());
        assert_eq!(
            map_executor_operation(ExecutorOperation::Insert { new: new.clone() }),
            MappedExecutorOperation::Data {
                op: Operation::Insert { new: new.clone() }
            }
        );
        assert_eq!(
            map_executor_operation(ExecutorOperation::Update {
                old: old.clone(),
                new: new.clone()
            }),
            MappedExecutorOperation::Data {
                op: Operation::Update {
                    old: old.clone(),
                    new
                }
            }
        );
        assert_eq!(
            map_executor_operation(ExecutorOperation::Delete { old: old.clone() }),
            MappedExecutorOperation::Data {
                op: Operation::Delete { old }
            }
        );
        assert_eq!(
            map_executor_operation(ExecutorOperation::Commit {
                epoch: epoch.clone()
            }),
            MappedExecutorOperation::Commit { epoch }
        );
        assert_eq!(
            map_executor_operation(ExecutorOperation::Terminate),
            MappedExecutorOperation::Terminate
        );
    }

    struct TestReceiverLoop {
        receivers: Vec<Receiver<ExecutorOperation>>,
        ops: Vec<(usize, Operation)>,
        commits: Vec<Epoch>,
        num_termations: usize,
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

        fn on_op(&mut self, index: usize, op: Operation) -> Result<(), ExecutionError> {
            self.ops.push((index, op));
            Ok(())
        }

        fn on_commit(&mut self, epoch: &Epoch) -> Result<(), ExecutionError> {
            self.commits.push(epoch.clone());
            Ok(())
        }

        fn on_terminate(&mut self) -> Result<(), ExecutionError> {
            self.num_termations += 1;
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
                    num_termations: 0,
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
        assert_eq!(test_loop.num_termations, 1);
    }

    #[test]
    fn receiver_loop_forwards_op() {
        let (mut test_loop, senders) = TestReceiverLoop::new(2);
        let record = Record::new(None, vec![Field::Int(1)], None);
        senders[0]
            .send(ExecutorOperation::Insert {
                new: record.clone(),
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
        let mut epoch0 = Epoch::new(0, details);
        let mut details = SourceStates::default();
        details.insert(
            NodeHandle::new(None, "1".to_string()),
            OpIdentifier::new(0, 0),
        );
        let mut epoch1 = Epoch::new(0, details);
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
            vec![Epoch::new(0, details.clone()), Epoch::new(1, details)]
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
        let epoch0 = Epoch::new(0, details);
        let mut details = SourceStates::new();
        details.insert(
            NodeHandle::new(None, "1".to_string()),
            OpIdentifier::new(0, 0),
        );
        let epoch1 = Epoch::new(1, details);
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
