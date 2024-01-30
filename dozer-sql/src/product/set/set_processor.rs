use super::operator::{SetAction, SetOperation};
use super::record_map::{
    AccurateCountingRecordMap, CountingRecordMap, CountingRecordMapEnum,
    ProbabilisticCountingRecordMap,
};
use crate::errors::{PipelineError, ProductError, SetError};
use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::checkpoint::serialize::Cursor;
use dozer_core::dozer_log::storage::Object;
use dozer_core::epoch::Epoch;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_recordstore::{ProcessorRecordStore, ProcessorRecordStoreDeserializer};
use dozer_types::errors::internal::BoxedError;
use dozer_types::types::{Operation, OperationWithId, Record};
use std::fmt::{Debug, Formatter};

pub struct SetProcessor {
    _id: String,
    /// Set operations
    operator: SetOperation,
    /// Hashmap containing records with its occurrence
    record_map: CountingRecordMapEnum,
}

impl SetProcessor {
    /// Creates a new [`SetProcessor`].
    pub fn new(
        id: String,
        operator: SetOperation,
        enable_probabilistic_optimizations: bool,
        record_store: &ProcessorRecordStoreDeserializer,
        checkpoint_data: Option<Vec<u8>>,
    ) -> Result<Self, SetError> {
        let mut cursor = checkpoint_data.as_deref().map(Cursor::new);
        Ok(Self {
            _id: id,
            operator,
            record_map: if enable_probabilistic_optimizations {
                ProbabilisticCountingRecordMap::new(cursor.as_mut())?.into()
            } else {
                AccurateCountingRecordMap::new(
                    cursor.as_mut().map(|cursor| (cursor, record_store)),
                )?
                .into()
            },
        })
    }

    fn delete(&mut self, record: Record) -> Result<Vec<(SetAction, Record)>, ProductError> {
        self.operator
            .execute(SetAction::Delete, record, &mut self.record_map)
            .map_err(|err| {
                ProductError::DeleteError("UNION query error:".to_string(), Box::new(err))
            })
    }

    fn insert(&mut self, record: Record) -> Result<Vec<(SetAction, Record)>, ProductError> {
        self.operator
            .execute(SetAction::Insert, record, &mut self.record_map)
            .map_err(|err| {
                ProductError::InsertError("UNION query error:".to_string(), Box::new(err))
            })
    }

    #[allow(clippy::type_complexity)]
    fn update(
        &mut self,
        old: Record,
        new: Record,
    ) -> Result<(Vec<(SetAction, Record)>, Vec<(SetAction, Record)>), ProductError> {
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
        op: OperationWithId,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        match op.op {
            Operation::Delete { old } => {
                let records = self.delete(old).map_err(PipelineError::ProductError)?;

                for (action, record) in records.into_iter() {
                    match action {
                        SetAction::Insert => {
                            fw.send(
                                OperationWithId::without_id(Operation::Insert { new: record }),
                                DEFAULT_PORT_HANDLE,
                            );
                        }
                        SetAction::Delete => {
                            fw.send(
                                OperationWithId::without_id(Operation::Delete { old: record }),
                                DEFAULT_PORT_HANDLE,
                            );
                        }
                    }
                }
            }
            Operation::Insert { new } => {
                let records = self.insert(new).map_err(PipelineError::ProductError)?;

                for (action, record) in records.into_iter() {
                    match action {
                        SetAction::Insert => {
                            fw.send(
                                OperationWithId::without_id(Operation::Insert { new: record }),
                                DEFAULT_PORT_HANDLE,
                            );
                        }
                        SetAction::Delete => {
                            fw.send(
                                OperationWithId::without_id(Operation::Delete { old: record }),
                                DEFAULT_PORT_HANDLE,
                            );
                        }
                    }
                }
            }
            Operation::Update { old, new } => {
                let (old_records, new_records) =
                    self.update(old, new).map_err(PipelineError::ProductError)?;

                for (action, old) in old_records.into_iter() {
                    match action {
                        SetAction::Insert => {
                            fw.send(
                                OperationWithId::without_id(Operation::Insert { new: old }),
                                DEFAULT_PORT_HANDLE,
                            );
                        }
                        SetAction::Delete => {
                            fw.send(
                                OperationWithId::without_id(Operation::Delete { old }),
                                DEFAULT_PORT_HANDLE,
                            );
                        }
                    }
                }

                for (action, new) in new_records.into_iter() {
                    match action {
                        SetAction::Insert => {
                            fw.send(
                                OperationWithId::without_id(Operation::Insert { new }),
                                DEFAULT_PORT_HANDLE,
                            );
                        }
                        SetAction::Delete => {
                            fw.send(
                                OperationWithId::without_id(Operation::Delete { old: new }),
                                DEFAULT_PORT_HANDLE,
                            );
                        }
                    }
                }
            }
            Operation::BatchInsert { new } => {
                for record in new {
                    self.process(
                        _from_port,
                        _record_store,
                        OperationWithId::without_id(Operation::Insert { new: record }),
                        fw,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn serialize(
        &mut self,
        record_store: &ProcessorRecordStore,
        mut object: Object,
    ) -> Result<(), BoxedError> {
        self.record_map
            .serialize(record_store, &mut object)
            .map_err(Into::into)
    }
}
