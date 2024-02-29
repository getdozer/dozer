use crate::errors::PipelineError;
use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::epoch::Epoch;
use dozer_core::node::Processor;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::errors::internal::BoxedError;
use dozer_types::types::{Operation, TableOperation};

use super::operator::WindowType;

#[derive(Debug)]
pub struct WindowProcessor {
    _id: String,
    window: WindowType,
}

impl WindowProcessor {
    pub fn new(id: String, window: WindowType) -> Self {
        Self { _id: id, window }
    }
}

impl Processor for WindowProcessor {
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
                let records = self
                    .window
                    .execute(old)
                    .map_err(PipelineError::WindowError)?;
                for record in records {
                    fw.send(TableOperation::without_id(
                        Operation::Delete { old: record },
                        DEFAULT_PORT_HANDLE,
                    ));
                }
            }
            Operation::Insert { new } => {
                let records = self
                    .window
                    .execute(new)
                    .map_err(PipelineError::WindowError)?;
                for record in records {
                    fw.send(TableOperation::without_id(
                        Operation::Insert { new: record },
                        DEFAULT_PORT_HANDLE,
                    ));
                }
            }
            Operation::Update { old, new } => {
                self.process(
                    TableOperation::without_id(Operation::Delete { old }, DEFAULT_PORT_HANDLE),
                    fw,
                )?;

                self.process(
                    TableOperation::without_id(Operation::Insert { new }, DEFAULT_PORT_HANDLE),
                    fw,
                )?;
            }
            Operation::BatchInsert { new } => {
                let mut records = vec![];
                for record in new {
                    records.extend(
                        self.window
                            .execute(record)
                            .map_err(PipelineError::WindowError)?,
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
