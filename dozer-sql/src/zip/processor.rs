use crate::errors::PipelineError;
use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::dozer_log::storage::Object;
use dozer_core::epoch::Epoch;
use dozer_core::node::Processor;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::errors::internal::BoxedError;
use dozer_types::types::{Operation, TableOperation};

use super::operator::WindowType;

#[derive(Debug)]
pub struct ZipProcessor {
    operator: ZipOperator,

impl ZipProcessor {
    pub fn new(id: String, ) -> Self {
        Self { _id: id }
    }
}

impl Processor for ZipProcessor {
    fn commit(&self, _epoch: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn process(
        &mut self,
        op: TableOperation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        match op.op {
            Operation::Delete { old } => {
                todo!()
            }
            Operation::Insert { new } => {
                todo!()
            }
            Operation::Update { old, new } => {
                todo!()
            }
            Operation::BatchInsert { new } => {
                todo!()
            }
        }
        Ok(())
    }

    fn serialize(&mut self, _object: Object) -> Result<(), BoxedError> {
        Ok(())
    }
}
