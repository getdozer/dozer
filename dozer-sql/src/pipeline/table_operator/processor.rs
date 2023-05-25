use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::epoch::Epoch;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::errors::internal::BoxedError;
use dozer_types::types::{Operation, Record, Schema};

use crate::pipeline::errors::{PipelineError, TableOperatorError};

use super::operator::{TableOperator, TableOperatorType};

#[derive(Debug)]
pub struct TableOperatorProcessor {
    operator: TableOperatorType,
    input_schema: Schema,
}

impl TableOperatorProcessor {
    pub fn new(operator: TableOperatorType, input_schema: Schema) -> Self {
        Self {
            operator,
            input_schema,
        }
    }

    fn execute(&self, record: &Record, schema: &Schema) -> Result<Vec<Record>, TableOperatorError> {
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
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        match op {
            Operation::Delete { ref old } => {
                let records = self
                    .execute(old, &self.input_schema)
                    .map_err(PipelineError::TableOperatorError)?;
                for record in records {
                    fw.send(Operation::Delete { old: record }, DEFAULT_PORT_HANDLE)?;
                }
            }
            Operation::Insert { ref new } => {
                let records = self
                    .execute(new, &self.input_schema)
                    .map_err(PipelineError::TableOperatorError)?;
                for record in records {
                    fw.send(Operation::Insert { new: record }, DEFAULT_PORT_HANDLE)?;
                }
            }
            Operation::Update { ref old, ref new } => {
                let old_records = self
                    .execute(old, &self.input_schema)
                    .map_err(PipelineError::TableOperatorError)?;
                for record in old_records {
                    fw.send(Operation::Delete { old: record }, DEFAULT_PORT_HANDLE)?;
                }

                let new_records = self
                    .execute(new, &self.input_schema)
                    .map_err(PipelineError::TableOperatorError)?;
                for record in new_records {
                    fw.send(Operation::Insert { new: record }, DEFAULT_PORT_HANDLE)?;
                }
            }
        }
        Ok(())
    }
}
