use dozer_core::{
    epoch::Epoch,
    errors::ExecutionError,
    node::{PortHandle, Sink, SinkFactory},
    record_store::RecordReader,
    storage::lmdb_storage::SharedTransaction,
    DEFAULT_PORT_HANDLE,
};
use dozer_sql::pipeline::builder::SchemaSQLContext;
use dozer_storage::lmdb_storage::LmdbExclusiveTransaction;
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

impl SinkFactory<SchemaSQLContext> for StreamingSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
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

    fn prepare(
        &self,
        _input_schemas: HashMap<PortHandle, (Schema, SchemaSQLContext)>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct StreamingSink {
    current: u64,
    sender: crossbeam::channel::Sender<Operation>,
}

impl Sink for StreamingSink {
    fn init(&mut self, _txn: &mut LmdbExclusiveTransaction) -> Result<(), ExecutionError> {
        debug!("SINK: Initialising StreamingSink");
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        _state: &SharedTransaction,
        _reader: &HashMap<PortHandle, Box<dyn RecordReader>>,
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
