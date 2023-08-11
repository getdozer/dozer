use crate::pipeline::errors::{PipelineError, ProductError};
use bloom::CountingBloomFilter;
use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::epoch::Epoch;
use dozer_core::executor_operation::ProcessorOperation;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::processor_record::{ProcessorRecord, ProcessorRecordStore};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::errors::internal::BoxedError;
use std::collections::hash_map::RandomState;
use std::fmt::{Debug, Formatter};

use super::operator::{SetAction, SetOperation};

pub struct SetProcessor {
    _id: String,
    /// Set operations
    operator: SetOperation,
    /// Hashmap containing records with its occurrence
    record_map: CountingBloomFilter,
}

const BITS_PER_ENTRY: usize = 8;
const FALSE_POSITIVE_RATE: f32 = 0.01;
const EXPECTED_NUM_ITEMS: u32 = 10000000;

impl SetProcessor {
    /// Creates a new [`SetProcessor`].
    pub fn new(id: String, operator: SetOperation) -> Result<Self, PipelineError> {
        let _s = RandomState::new();
        Ok(Self {
            _id: id,
            operator,
            record_map: CountingBloomFilter::with_rate(
                BITS_PER_ENTRY,
                FALSE_POSITIVE_RATE,
                EXPECTED_NUM_ITEMS,
            ),
        })
    }

    fn delete(
        &mut self,
        record: ProcessorRecord,
    ) -> Result<Vec<(SetAction, ProcessorRecord)>, ProductError> {
        self.operator
            .execute(SetAction::Delete, record, &mut self.record_map)
            .map_err(|err| {
                ProductError::DeleteError("UNION query error:".to_string(), Box::new(err))
            })
    }

    fn insert(
        &mut self,
        record: ProcessorRecord,
    ) -> Result<Vec<(SetAction, ProcessorRecord)>, ProductError> {
        self.operator
            .execute(SetAction::Insert, record, &mut self.record_map)
            .map_err(|err| {
                ProductError::InsertError("UNION query error:".to_string(), Box::new(err))
            })
    }

    #[allow(clippy::type_complexity)]
    fn update(
        &mut self,
        old: ProcessorRecord,
        new: ProcessorRecord,
    ) -> Result<
        (
            Vec<(SetAction, ProcessorRecord)>,
            Vec<(SetAction, ProcessorRecord)>,
        ),
        ProductError,
    > {
        let old_records = self
            .operator
            .execute(SetAction::Delete, old, &mut self.record_map)
            .map_err(|err| {
                ProductError::UpdateOldError("UNION query error:".to_string(), Box::new(err))
            })?;

        let new_records = self
            .operator
            .execute(SetAction::Insert, new, &mut self.record_map)
            .map_err(|err| {
                ProductError::UpdateNewError("UNION query error:".to_string(), Box::new(err))
            })?;

        Ok((old_records, new_records))
    }
}

impl Debug for SetProcessor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SetProcessor").field(&self.operator).finish()
    }
}

impl Processor for SetProcessor {
    fn commit(&self, _epoch: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        _record_store: &ProcessorRecordStore,
        op: ProcessorOperation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        match op {
            ProcessorOperation::Delete { old } => {
                let records = self.delete(old).map_err(PipelineError::ProductError)?;

                for (action, record) in records.into_iter() {
                    match action {
                        SetAction::Insert => {
                            fw.send(
                                ProcessorOperation::Insert { new: record },
                                DEFAULT_PORT_HANDLE,
                            );
                        }
                        SetAction::Delete => {
                            fw.send(
                                ProcessorOperation::Delete { old: record },
                                DEFAULT_PORT_HANDLE,
                            );
                        }
                    }
                }
            }
            ProcessorOperation::Insert { new } => {
                let records = self.insert(new).map_err(PipelineError::ProductError)?;

                for (action, record) in records.into_iter() {
                    match action {
                        SetAction::Insert => {
                            fw.send(
                                ProcessorOperation::Insert { new: record },
                                DEFAULT_PORT_HANDLE,
                            );
                        }
                        SetAction::Delete => {
                            fw.send(
                                ProcessorOperation::Delete { old: record },
                                DEFAULT_PORT_HANDLE,
                            );
                        }
                    }
                }
            }
            ProcessorOperation::Update { old, new } => {
                let (old_records, new_records) =
                    self.update(old, new).map_err(PipelineError::ProductError)?;

                for (action, old) in old_records.into_iter() {
                    match action {
                        SetAction::Insert => {
                            fw.send(ProcessorOperation::Insert { new: old }, DEFAULT_PORT_HANDLE);
                        }
                        SetAction::Delete => {
                            fw.send(ProcessorOperation::Delete { old }, DEFAULT_PORT_HANDLE);
                        }
                    }
                }

                for (action, new) in new_records.into_iter() {
                    match action {
                        SetAction::Insert => {
                            fw.send(ProcessorOperation::Insert { new }, DEFAULT_PORT_HANDLE);
                        }
                        SetAction::Delete => {
                            fw.send(ProcessorOperation::Delete { old: new }, DEFAULT_PORT_HANDLE);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
