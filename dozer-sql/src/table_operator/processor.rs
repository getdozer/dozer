use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::epoch::Epoch;
use dozer_core::node::Processor;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::errors::internal::BoxedError;
use dozer_types::types::{Operation, Schema, TableOperation};

use crate::errors::PipelineError;

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
}

impl Processor for TableOperatorProcessor {
    fn commit(&self, _epoch: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn process(
        &mut self,
        op: TableOperation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        match op.op {
            Operation::Delete { ref old } => {
                let records = self
                    .operator
                    .execute(old, &self.input_schema)
                    .map_err(PipelineError::TableOperatorError)?;
                for record in records {
                    fw.send(TableOperation::without_id(
                        Operation::Delete { old: record },
                        DEFAULT_PORT_HANDLE,
                    ));
                }
            }
            Operation::Insert { ref new } => {
                let records = self
                    .operator
                    .execute(new, &self.input_schema)
                    .map_err(PipelineError::TableOperatorError)?;
                for record in records {
                    fw.send(TableOperation::without_id(
                        Operation::Insert { new: record },
                        DEFAULT_PORT_HANDLE,
                    ));
                }
            }
            Operation::Update { ref old, ref new } => {
                let old_records = self
                    .operator
                    .execute(old, &self.input_schema)
                    .map_err(PipelineError::TableOperatorError)?;
                for record in old_records {
                    fw.send(TableOperation::without_id(
                        Operation::Delete { old: record },
                        DEFAULT_PORT_HANDLE,
                    ));
                }

                let new_records = self
                    .operator
                    .execute(new, &self.input_schema)
                    .map_err(PipelineError::TableOperatorError)?;
                for record in new_records {
                    fw.send(TableOperation::without_id(
                        Operation::Insert { new: record },
                        DEFAULT_PORT_HANDLE,
                    ));
                }
            }
            Operation::BatchInsert { new } => {
                let mut records = vec![];
                for record in new {
                    records.extend(
                        self.operator
                            .execute(&record, &self.input_schema)
                            .map_err(PipelineError::TableOperatorError)?,
                    );
                }
                fw.send(TableOperation::without_id(
                    Operation::BatchInsert { new: records },
                    DEFAULT_PORT_HANDLE,
                ));
            }
        }
        Ok(())
    }
}
