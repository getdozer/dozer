use crate::pipeline::errors::{PipelineError, WindowError};
use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::epoch::Epoch;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::errors::internal::BoxedError;
use dozer_types::types::{Operation, Record};

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

    fn execute(&self, record: &Record) -> Result<Vec<Record>, WindowError> {
        self.window.execute(record)
    }
}

impl Processor for WindowProcessor {
    fn commit(&mut self, _epoch: &Epoch) -> Result<(), BoxedError> {
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
                let records = self.execute(old).map_err(PipelineError::WindowError)?;
                for record in records {
                    fw.send(Operation::Delete { old: record }, DEFAULT_PORT_HANDLE);
                }
            }
            Operation::Insert { ref new } => {
                let records = self.execute(new).map_err(PipelineError::WindowError)?;
                for record in records {
                    fw.send(Operation::Insert { new: record }, DEFAULT_PORT_HANDLE);
                }
            }
            Operation::Update { ref old, ref new } => {
                let old_records = self.execute(old).map_err(PipelineError::WindowError)?;
                for record in old_records {
                    fw.send(Operation::Delete { old: record }, DEFAULT_PORT_HANDLE);
                }

                let new_records = self.execute(new).map_err(PipelineError::WindowError)?;
                for record in new_records {
                    fw.send(Operation::Insert { new: record }, DEFAULT_PORT_HANDLE);
                }
            }
        }
        Ok(())
    }
}
