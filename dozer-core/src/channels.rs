use crate::errors::ExecutionError;
use crate::node::PortHandle;
use core::marker::{Send, Sync};
use core::result::Result;
use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::types::ProcessorOperation;

pub trait SourceChannelForwarder: Send + Sync {
    fn send(&mut self, message: IngestionMessage, port: PortHandle) -> Result<(), ExecutionError>;
}

pub trait ProcessorChannelForwarder {
    /// Sends a operation to downstream nodes. Panics if the operation cannot be sent.
    ///
    /// We must panic instead of returning an error because this method will be called by `Processor::process`,
    /// which only returns recoverable errors.
    fn send(&mut self, op: ProcessorOperation, port: PortHandle);
}
