use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::epoch::Epoch;
use dozer_core::errors::ExecutionError;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::types::Operation;

#[derive(Debug)]
pub struct TableProcessor {}

impl TableProcessor {
    pub fn new() -> Self {
        Self {}
    }
}

impl Processor for TableProcessor {
    fn commit(&self, _epoch: &Epoch) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), ExecutionError> {
        fw.send(op, DEFAULT_PORT_HANDLE)?;
        Ok(())
    }
}
