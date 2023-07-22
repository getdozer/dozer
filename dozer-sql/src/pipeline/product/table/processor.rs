use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::epoch::Epoch;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::errors::internal::BoxedError;
use dozer_types::types::Operation;

#[derive(Debug)]
pub struct TableProcessor {
    _id: String,
}

impl TableProcessor {
    pub fn new(id: String) -> Self {
        Self { _id: id }
    }
}

impl Processor for TableProcessor {
    fn commit(&mut self, _epoch: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        fw.send(op, DEFAULT_PORT_HANDLE);
        Ok(())
    }
}
