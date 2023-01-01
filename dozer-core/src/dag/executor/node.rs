use std::fmt::Debug;

use crate::dag::errors::ExecutionError;

use super::receiver_loop::ReceiverLoop;

/// A node in the execution DAG.
pub trait Node: Debug {
    /// Runs the node.
    fn run(self) -> Result<(), ExecutionError>;
}

impl<T: ReceiverLoop + Debug> Node for T {
    fn run(mut self) -> Result<(), ExecutionError> {
        self.receiver_loop()
    }
}
