use crate::node::PortHandle;
use dozer_types::types::OperationWithId;

pub trait ProcessorChannelForwarder {
    /// Sends a operation to downstream nodes. Panics if the operation cannot be sent.
    ///
    /// We must panic instead of returning an error because this method will be called by `Processor::process`,
    /// which only returns recoverable errors.
    fn send(&mut self, op: OperationWithId, port: PortHandle);
}
