use crate::core::node::PortHandle;
use crate::errors::execution::ExecutionError;
use crate::types::{Operation, Schema};
use core::marker::{Send, Sync};
use core::result::Result;

pub trait SourceChannelForwarder: Send + Sync {
    fn send(&self, seq: u64, op: Operation, port: PortHandle) -> Result<(), ExecutionError>;
    fn update_schema(&self, schema: Schema, port: PortHandle) -> Result<(), ExecutionError>;
}

pub trait ProcessorChannelForwarder {
    fn send(&self, op: Operation, port: PortHandle) -> Result<(), ExecutionError>;
}

pub trait ChannelManager {
    fn terminate(&self) -> Result<(), ExecutionError>;
}
