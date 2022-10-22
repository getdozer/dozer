use crate::dag::dag::PortHandle;
use crate::dag::error::ExecutionError;
use crate::dag::forwarder::{ChannelManager, ProcessorChannelForwarder, SourceChannelForwarder};
use crate::state::{StateStore, StateStoreOptions};
use dozer_types::types::{Operation, Schema};
use std::collections::HashMap;

pub trait ExecutionContext: Send + Sync {}

pub trait ProcessorFactory: Send + Sync {
    fn get_state_store_opts(&self) -> Option<StateStoreOptions>;
    fn get_input_ports(&self) -> Vec<PortHandle>;
    fn get_output_ports(&self) -> Vec<PortHandle>;
    fn build(&self) -> Box<dyn Processor>;
}

pub trait Processor {
    fn update_schema(
        &mut self,
        output_port: PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, ExecutionError>;
    fn init(&mut self, state: &mut dyn StateStore) -> Result<(), ExecutionError>;
    fn process(
        &mut self,
        from_port: PortHandle,
        op: Operation,
        fw: &dyn ProcessorChannelForwarder,
        state: &mut dyn StateStore,
    ) -> Result<(), ExecutionError>;
}

pub trait SourceFactory: Send + Sync {
    fn get_state_store_opts(&self) -> Option<StateStoreOptions>;
    fn get_output_ports(&self) -> Vec<PortHandle>;
    fn build(&self) -> Box<dyn Source>;
}

pub trait Source {
    fn get_output_schema(&self, port: PortHandle) -> Schema;
    fn start(
        &self,
        fw: &dyn SourceChannelForwarder,
        cm: &dyn ChannelManager,
        state: &mut dyn StateStore,
        from_seq: Option<u64>,
    ) -> Result<(), ExecutionError>;
}

pub trait SinkFactory: Send + Sync {
    fn get_state_store_opts(&self) -> Option<StateStoreOptions>;
    fn get_input_ports(&self) -> Vec<PortHandle>;
    fn build(&self) -> Box<dyn Sink>;
}

pub trait Sink {
    fn update_schema(
        &mut self,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<(), ExecutionError>;
    fn init(&mut self, state: &mut dyn StateStore) -> Result<(), ExecutionError>;
    fn process(
        &mut self,
        from_port: PortHandle,
        seq: u64,
        op: Operation,
        state: &mut dyn StateStore,
    ) -> Result<(), ExecutionError>;
}
