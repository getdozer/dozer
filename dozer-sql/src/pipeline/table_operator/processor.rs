use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::epoch::Epoch;
use dozer_core::executor_operation::ProcessorOperation;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::processor_record::ProcessorRecord;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::errors::internal::BoxedError;
use dozer_types::types::Schema;

use crate::pipeline::errors::{PipelineError, TableOperatorError};

use super::operator::{TableOperator, TableOperatorType};

#[derive(Debug)]
pub struct TableOperatorProcessor {
    _id: String,
    operator: TableOperatorType,
    input_schema: Schema,
}

impl TableOperatorProcessor {
    pub fn new(id: String, operator: TableOperatorType, input_schema: Schema) -> Self {
        Self {
            _id: id,
            operator,
            input_schema,
        }
    }

    fn execute(
        &self,
        record: &ProcessorRecord,
        schema: &Schema,
    ) -> Result<Vec<ProcessorRecord>, TableOperatorError> {
        self.operator.execute(record, schema)
    }
}

impl Processor for TableOperatorProcessor {
    fn commit(&self, _epoch: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: ProcessorOperation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        match op {
            ProcessorOperation::Delete { ref old } => {
                let records = self
                    .execute(old, &self.input_schema)
                    .map_err(PipelineError::TableOperatorError)?;
                for record in records {
                    fw.send(
                        ProcessorOperation::Delete { old: record },
                        DEFAULT_PORT_HANDLE,
                    );
                }
            }
            ProcessorOperation::Insert { ref new } => {
                let records = self
                    .execute(new, &self.input_schema)
                    .map_err(PipelineError::TableOperatorError)?;
                for record in records {
                    fw.send(
                        ProcessorOperation::Insert { new: record },
                        DEFAULT_PORT_HANDLE,
                    );
                }
            }
            ProcessorOperation::Update { ref old, ref new } => {
                let old_records = self
                    .execute(old, &self.input_schema)
                    .map_err(PipelineError::TableOperatorError)?;
                for record in old_records {
                    fw.send(
                        ProcessorOperation::Delete { old: record },
                        DEFAULT_PORT_HANDLE,
                    );
                }

                let new_records = self
                    .execute(new, &self.input_schema)
                    .map_err(PipelineError::TableOperatorError)?;
                for record in new_records {
                    fw.send(
                        ProcessorOperation::Insert { new: record },
                        DEFAULT_PORT_HANDLE,
                    );
                }
            }
        }
        Ok(())
    }
}
