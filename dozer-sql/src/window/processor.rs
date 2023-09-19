use crate::errors::PipelineError;
use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::dozer_log::storage::Object;
use dozer_core::epoch::Epoch;
use dozer_core::executor_operation::ProcessorOperation;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_recordstore::ProcessorRecordStore;
use dozer_types::errors::internal::BoxedError;

use super::operator::WindowType;

#[derive(Debug)]
pub struct WindowProcessor {
    _id: String,
    window: WindowType,
}

impl WindowProcessor {
    pub fn new(id: String, window: WindowType, _checkpoint_data: Option<Vec<u8>>) -> Self {
        Self { _id: id, window }
    }
}

impl Processor for WindowProcessor {
    fn commit(&self, _epoch: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        record_store: &ProcessorRecordStore,
        op: ProcessorOperation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        match op {
            ProcessorOperation::Delete { old } => {
                let old_decoded = record_store.load_record(&old)?;
                let records = self
                    .window
                    .execute(record_store, old, old_decoded)
                    .map_err(PipelineError::WindowError)?;
                for record in records {
                    fw.send(
                        ProcessorOperation::Delete { old: record },
                        DEFAULT_PORT_HANDLE,
                    );
                }
            }
            ProcessorOperation::Insert { new } => {
                let new_decoded = record_store.load_record(&new)?;
                let records = self
                    .window
                    .execute(record_store, new, new_decoded)
                    .map_err(PipelineError::WindowError)?;
                for record in records {
                    fw.send(
                        ProcessorOperation::Insert { new: record },
                        DEFAULT_PORT_HANDLE,
                    );
                }
            }
            ProcessorOperation::Update { old, new } => {
                self.process(
                    DEFAULT_PORT_HANDLE,
                    record_store,
                    ProcessorOperation::Delete { old },
                    fw,
                )?;

                self.process(
                    DEFAULT_PORT_HANDLE,
                    record_store,
                    ProcessorOperation::Insert { new },
                    fw,
                )?;
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
