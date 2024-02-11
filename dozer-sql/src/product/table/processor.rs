use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::dozer_log::storage::Object;
use dozer_core::epoch::Epoch;
use dozer_core::node::Processor;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::errors::internal::BoxedError;
use dozer_types::types::TableOperation;

#[derive(Debug)]
pub struct TableProcessor {
    _id: String,
}

impl TableProcessor {
    pub fn new(id: String, _checkpoint_data: Option<Vec<u8>>) -> Self {
        Self { _id: id }
    }
}

impl Processor for TableProcessor {
    fn commit(&self, _epoch: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn process(
        &mut self,
        mut op: TableOperation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        op.port = DEFAULT_PORT_HANDLE;
        fw.send(op);
        Ok(())
    }

    fn serialize(&mut self, _object: Object) -> Result<(), BoxedError> {
        Ok(())
    }
}
