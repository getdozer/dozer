use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::dozer_log::storage::Object;
use dozer_core::epoch::Epoch;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_recordstore::ProcessorRecordStore;
use dozer_types::errors::internal::BoxedError;
use dozer_types::types::{Operation, Schema};

use crate::errors::PipelineError;

use super::operator::{TableOperator, TableOperatorType};

#[derive(Debug)]
pub struct TableOperatorProcessor {
    _id: String,
    operator: TableOperatorType,
    input_schema: Schema,
}

impl TableOperatorProcessor {
    pub fn new(
        id: String,
        operator: TableOperatorType,
        input_schema: Schema,
        _checkpoint_data: Option<Vec<u8>>,
    ) -> Self {
        Self {
            _id: id,
            operator,
            input_schema,
        }
    }
}

impl Processor for TableOperatorProcessor {
    fn commit(&self, _epoch: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        record_store: &ProcessorRecordStore,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        match op {
            Operation::Delete { ref old } => {
                let records = self
                    .operator
                    .execute(record_store, old, &self.input_schema)
                    .map_err(PipelineError::TableOperatorError)?;
                for record in records {
                    fw.send(Operation::Delete { old: record }, DEFAULT_PORT_HANDLE);
                }
            }
            Operation::Insert { ref new } => {
                let records = self
                    .operator
                    .execute(record_store, new, &self.input_schema)
                    .map_err(PipelineError::TableOperatorError)?;
                for record in records {
                    fw.send(Operation::Insert { new: record }, DEFAULT_PORT_HANDLE);
                }
            }
            Operation::Update { ref old, ref new } => {
                let old_records = self
                    .operator
                    .execute(record_store, old, &self.input_schema)
                    .map_err(PipelineError::TableOperatorError)?;
                for record in old_records {
                    fw.send(Operation::Delete { old: record }, DEFAULT_PORT_HANDLE);
                }

                let new_records = self
                    .operator
                    .execute(record_store, new, &self.input_schema)
                    .map_err(PipelineError::TableOperatorError)?;
                for record in new_records {
                    fw.send(Operation::Insert { new: record }, DEFAULT_PORT_HANDLE);
                }
            }
            Operation::BatchInsert { new } => {
                for record in new {
                    self.process(
                        _from_port,
                        record_store,
                        Operation::Insert { new: record },
                        fw,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn serialize(
        &mut self,
        _record_store: &ProcessorRecordStore,
        _object: Object,
    ) -> Result<(), BoxedError> {
        Ok(())
    }
}
