use crate::pipeline::errors::{PipelineError, ProductError};
use crate::pipeline::product::set::{SetAction, SetOperation};
use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::epoch::Epoch;
use dozer_core::errors::ExecutionError;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::storage::lmdb_storage::SharedTransaction;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::types::{Operation, Record};
use hashbrown::HashMap;
use std::collections::hash_map::RandomState;

#[derive(Debug)]
pub struct SetProcessor {
    /// Set operations
    operator: SetOperation,
    /// Hashmap containing records with its occurrence
    record_map: HashMap<Record, usize>,
}

impl SetProcessor {
    /// Creates a new [`SetProcessor`].
    pub fn new(operator: SetOperation) -> Result<Self, PipelineError> {
        let _s = RandomState::new();
        Ok(Self {
            operator,
            record_map: HashMap::new(),
        })
    }

    fn delete(&mut self, record: &Record) -> Result<Vec<(SetAction, Record)>, ProductError> {
        self.operator
            .execute(SetAction::Delete, record, &mut self.record_map)
            .map_err(|err| {
                ProductError::DeleteError("UNION query error:".to_string(), Box::new(err))
            })
    }

    fn insert(&mut self, record: &Record) -> Result<Vec<(SetAction, Record)>, ProductError> {
        self.operator
            .execute(SetAction::Insert, record, &mut self.record_map)
            .map_err(|err| {
                ProductError::InsertError("UNION query error:".to_string(), Box::new(err))
            })
    }

    #[allow(clippy::type_complexity)]
    fn update(
        &mut self,
        old: &Record,
        new: &Record,
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

impl Processor for SetProcessor {
    fn commit(&self, _epoch: &Epoch, _tx: &SharedTransaction) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
        _transaction: &SharedTransaction,
    ) -> Result<(), ExecutionError> {
        match op {
            Operation::Delete { ref old } => {
                let records = self
                    .delete(old)
                    .map_err(|err| ExecutionError::ProductProcessorError(Box::new(err)))?;

                for (action, record) in records.into_iter() {
                    match action {
                        SetAction::Insert => {
                            let _ = fw.send(Operation::Insert { new: record }, DEFAULT_PORT_HANDLE);
                        }
                        SetAction::Delete => {
                            let _ = fw.send(Operation::Delete { old: record }, DEFAULT_PORT_HANDLE);
                        }
                    }
                }
            }
            Operation::Insert { ref new } => {
                let records = self
                    .insert(new)
                    .map_err(|err| ExecutionError::ProductProcessorError(Box::new(err)))?;

                for (action, record) in records.into_iter() {
                    match action {
                        SetAction::Insert => {
                            let _ = fw.send(Operation::Insert { new: record }, DEFAULT_PORT_HANDLE);
                        }
                        SetAction::Delete => {
                            let _ = fw.send(Operation::Delete { old: record }, DEFAULT_PORT_HANDLE);
                        }
                    }
                }
            }
            Operation::Update { ref old, ref new } => {
                let (old_records, new_records) = self
                    .update(old, new)
                    .map_err(|err| ExecutionError::ProductProcessorError(Box::new(err)))?;

                for (action, old) in old_records.into_iter() {
                    match action {
                        SetAction::Insert => {
                            let _ = fw.send(Operation::Insert { new: old }, DEFAULT_PORT_HANDLE);
                        }
                        SetAction::Delete => {
                            let _ = fw.send(Operation::Delete { old }, DEFAULT_PORT_HANDLE);
                        }
                    }
                }

                for (action, new) in new_records.into_iter() {
                    match action {
                        SetAction::Insert => {
                            let _ = fw.send(Operation::Insert { new }, DEFAULT_PORT_HANDLE);
                        }
                        SetAction::Delete => {
                            let _ = fw.send(Operation::Delete { old: new }, DEFAULT_PORT_HANDLE);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
