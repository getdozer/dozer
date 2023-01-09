use dozer_core::{
    dag::{
        dag::DEFAULT_PORT_HANDLE,
        epoch::Epoch,
        errors::ExecutionError,
        node::{PortHandle, Sink, SinkFactory},
        record_store::RecordReader,
    },
    storage::lmdb_storage::{LmdbEnvironmentManager, SharedTransaction},
};
use dozer_types::{
    crossbeam,
    log::debug,
    types::{Operation, Schema},
};
use std::collections::HashMap;

#[derive(Debug)]
pub(crate) struct StreamingSinkFactory {
    sender: crossbeam::channel::Sender<Operation>,
}

impl StreamingSinkFactory {
    pub fn new(sender: crossbeam::channel::Sender<Operation>) -> Self {
        Self { sender }
    }
}

impl SinkFactory for StreamingSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn set_input_schema(
        &self,
        _input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, ExecutionError> {
        Ok(Box::new(StreamingSink {
            current: 0,
            sender: self.sender.clone(),
        }))
    }

    fn prepare(&self, _input_schemas: HashMap<PortHandle, Schema>) -> Result<(), ExecutionError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct StreamingSink {
    current: u64,
    sender: crossbeam::channel::Sender<Operation>,
}

impl Sink for StreamingSink {
    fn init(&mut self, _env: &mut LmdbEnvironmentManager) -> Result<(), ExecutionError> {
        debug!("SINK: Initialising StreamingSink");
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        _state: &SharedTransaction,
        _reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<(), ExecutionError> {
        self.current += 1;
        let _res = self
            .sender
            .try_send(op)
            .map_err(|e| ExecutionError::InternalError(Box::new(e)));

        Ok(())
    }

    fn commit(&mut self, _epoch: &Epoch, _tx: &SharedTransaction) -> Result<(), ExecutionError> {
        Ok(())
    }
}
