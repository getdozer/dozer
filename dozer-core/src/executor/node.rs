use std::fmt::Debug;

use crate::errors::ExecutionError;

use super::receiver_loop::ReceiverLoop;

/// A node in the execution DAG.
pub trait Node {
    /// Runs the node.
    fn run(self) -> Result<(), ExecutionError>;
}

impl<T: ReceiverLoop + Debug> Node for T {
    fn run(self) -> Result<(), ExecutionError> {
        let initial_epoch_id = self.initial_epoch_id();
        self.receiver_loop(initial_epoch_id)
    }
}
