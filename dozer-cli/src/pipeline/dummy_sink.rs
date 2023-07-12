use std::collections::HashMap;

use dozer_core::{
    node::{PortHandle, Sink, SinkFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_types::{
    epoch::Epoch,
    errors::internal::BoxedError,
    types::{Operation, Schema},
};

#[derive(Debug)]
pub struct DummySinkFactory;

impl<T> SinkFactory<T> for DummySinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn prepare(&self, _input_schemas: HashMap<PortHandle, (Schema, T)>) -> Result<(), BoxedError> {
        Ok(())
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, BoxedError> {
        Ok(Box::new(DummySink))
    }
}

#[derive(Debug)]
struct DummySink;

impl Sink for DummySink {
    fn process(&mut self, _from_port: PortHandle, _op: Operation) -> Result<(), BoxedError> {
        Ok(())
    }

    fn commit(&mut self, _epoch_details: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn on_source_snapshotting_done(&mut self, _connection_name: String) -> Result<(), BoxedError> {
        Ok(())
    }
}
