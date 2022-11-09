use crate::dag::errors::ExecutionError;
use crate::dag::node::PortHandle;
use crate::storage::common::{RenewableRwTransaction, RwTransaction};
use core::marker::{Send, Sync};
use core::result::Result;
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Operation, Schema};
use std::sync::Arc;

pub trait SourceChannelForwarder: Send + Sync {
    fn send(&mut self, seq: u64, op: Operation, port: PortHandle) -> Result<(), ExecutionError>;
    fn update_schema(&mut self, schema: Schema, port: PortHandle) -> Result<(), ExecutionError>;
    fn terminate(&mut self) -> Result<(), ExecutionError>;
}

pub trait ProcessorChannelForwarder {
    fn send(
        &mut self,
        tx: Option<&mut dyn RwTransaction>,
        op: Operation,
        port: PortHandle,
    ) -> Result<(), ExecutionError>;
}
