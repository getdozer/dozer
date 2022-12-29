use std::{borrow::Cow, collections::HashMap};

use crossbeam::channel::Receiver;
use dozer_types::{internal_err, types::Operation};
use log::info;

use crate::dag::{
    epoch::Epoch,
    errors::ExecutionError::{self, InternalError},
    executor_utils::init_select,
};

use super::{name::Name, ExecutorOperation, InputPortState};

#[derive(Debug)]
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
        ExecutorOperation::Commit { epoch_details } => MappedExecutorOperation::Commit {
            epoch: epoch_details,
        },
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
        let mut port_states = vec![InputPortState::Open; receivers.len()];

        let mut commits_received: usize = 0;
        let mut common_epoch = Epoch::new(0, HashMap::new());

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
                        common_epoch = Epoch::new(common_epoch.id + 1, HashMap::new());
                        commits_received = 0;
                        sel = init_select(&receivers);
                    }
                }
                MappedExecutorOperation::Terminate => {
                    port_states[index] = InputPortState::Terminated;
                    sel.remove(index);
                    info!(
                        "[{}] Received Terminate request on port {}",
                        self.name(),
                        self.receiver_name(index)
                    );
                    if port_states.iter().all(|v| v == &InputPortState::Terminated) {
                        self.on_terminate()?;
                        info!("[{}] Quit", self.name());
                        return Ok(());
                    }
                }
            }
        }
    }
}
