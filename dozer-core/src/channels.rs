use crate::errors::ExecutionError;
use crate::node::PortHandle;
use core::marker::{Send, Sync};
use core::result::Result;
use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::types::Operation;

pub trait SourceChannelForwarder: Send + Sync {
    fn send(&mut self, message: IngestionMessage, port: PortHandle) -> Result<(), ExecutionError>;
}

pub trait ProcessorChannelForwarder {
    fn send(&mut self, op: Vec<Operation>, port: PortHandle) -> Result<(), ExecutionError>;
}
