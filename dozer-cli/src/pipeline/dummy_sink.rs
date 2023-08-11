use std::collections::HashMap;

use dozer_core::{
    epoch::Epoch,
    executor_operation::ProcessorOperation,
    node::{PortHandle, Sink, SinkFactory},
    processor_record::ProcessorRecordStore,
    DEFAULT_PORT_HANDLE,
};
use dozer_types::{errors::internal::BoxedError, types::Schema};

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
    fn process(
        &mut self,
        _from_port: PortHandle,
        _record_store: &ProcessorRecordStore,
        _op: ProcessorOperation,
    ) -> Result<(), BoxedError> {
        Ok(())
    }

    fn commit(&mut self, _epoch_details: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn on_source_snapshotting_done(&mut self, _connection_name: String) -> Result<(), BoxedError> {
        Ok(())
    }
}
