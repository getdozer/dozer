use crate::dag::errors::ExecutionError;
use crate::dag::node::PortHandle;
use core::marker::{Send, Sync};
use core::result::Result;
use dozer_types::types::{Operation, Schema};

pub trait SourceChannelForwarder: Send + Sync {
    fn send(&mut self, seq: u64, op: Operation, port: PortHandle) -> Result<(), ExecutionError>;
    fn update_schema(&mut self, schema: Schema, port: PortHandle) -> Result<(), ExecutionError>;
    fn terminate(&mut self) -> Result<(), ExecutionError>;
}

pub trait ProcessorChannelForwarder {
    fn send(&mut self, op: Operation, port: PortHandle) -> Result<(), ExecutionError>;
}
