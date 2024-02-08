use std::fmt::Debug;

use crate::errors::ExecutionError;

use super::receiver_loop::ReceiverLoop;

/// A node in the execution DAG.
pub trait Node {
    /// Runs the node.
    fn run(self) -> Result<(), ExecutionError>;
}

impl<T: ReceiverLoop + Debug> Node for T {
    fn run(mut self) -> Result<(), ExecutionError> {
        self.receiver_loop(self.initial_epoch_id())
    }
}
