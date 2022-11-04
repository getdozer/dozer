use crate::dag::errors::ExecutionError;
use crate::dag::node::PortHandle;
use core::marker::{Send, Sync};
use core::result::Result;
use dozer_types::types::{Operation, Schema};

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
