use crate::errors::ExecutionError;
use crate::node::PortHandle;
use core::marker::{Send, Sync};
use core::result::Result;
use dozer_types::types::Operation;

pub trait SourceChannelForwarder: Send + Sync {
    fn send(
        &mut self,
        txid: u64,
        seq_in_tx: u64,
        op: Operation,
        port: PortHandle,
    ) -> Result<(), ExecutionError>;
}

pub trait ProcessorChannelForwarder {
    fn send(&mut self, op: Operation, port: PortHandle) -> Result<(), ExecutionError>;
}
