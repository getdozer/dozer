use crate::pipeline::errors::{PipelineError, WindowError};
use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::epoch::Epoch;
use dozer_core::executor_operation::ProcessorOperation;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::processor_record::ProcessorRecord;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::errors::internal::BoxedError;

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

    fn execute(&self, record: ProcessorRecord) -> Result<Vec<ProcessorRecord>, WindowError> {
        self.window.execute(record)
    }
}

impl Processor for WindowProcessor {
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
            ProcessorOperation::Delete { old } => {
                let records = self.execute(old).map_err(PipelineError::WindowError)?;
                for record in records {
                    fw.send(
                        ProcessorOperation::Delete { old: record },
                        DEFAULT_PORT_HANDLE,
                    );
                }
            }
            ProcessorOperation::Insert { new } => {
                let records = self.execute(new).map_err(PipelineError::WindowError)?;
                for record in records {
                    fw.send(
                        ProcessorOperation::Insert { new: record },
                        DEFAULT_PORT_HANDLE,
                    );
                }
            }
            ProcessorOperation::Update { old, new } => {
                let old_records = self.execute(old).map_err(PipelineError::WindowError)?;
                for record in old_records {
                    fw.send(
                        ProcessorOperation::Delete { old: record },
                        DEFAULT_PORT_HANDLE,
                    );
                }

                let new_records = self.execute(new).map_err(PipelineError::WindowError)?;
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
