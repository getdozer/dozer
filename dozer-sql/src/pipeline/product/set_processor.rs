use crate::pipeline::errors::{PipelineError, ProductError, SetError};
use crate::pipeline::product::set::{SetAction, SetOperation};
use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::epoch::Epoch;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::record_store::RecordReader;
use dozer_core::storage::lmdb_storage::{LmdbExclusiveTransaction, SharedTransaction};
use dozer_core::DEFAULT_PORT_HANDLE;

use dozer_core::errors::ExecutionError;
use dozer_core::errors::ExecutionError::InternalError;
use dozer_types::types::{Operation, Record};
use lmdb::{Database, DatabaseFlags};
use std::collections::HashMap;

#[derive(Debug)]
pub struct SetProcessor {
    /// Set operations
    operator: SetOperation,
    /// Database to store Join indexes
    db: Option<Database>,
}

impl SetProcessor {
    /// Creates a new [`FromProcessor`].
    pub fn new(operator: SetOperation) -> Self {
        Self { operator, db: None }
    }

    fn init_store(&mut self, txn: &mut LmdbExclusiveTransaction) -> Result<(), PipelineError> {
        self.db = Some(txn.create_database(Some("set"), Some(DatabaseFlags::DUP_SORT))?);

        Ok(())
    }

    fn delete(
        &mut self,
        _from_port: PortHandle,
        record: &Record,
        txn: &SharedTransaction,
        _reader: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<(SetAction, Record)>, ProductError> {
        let database = if self.db.is_some() {
            self.db
                .map_or(Err(SetError::DatabaseUnavailable), Ok)
                .unwrap()
        } else {
            return Err(ProductError::InvalidDatabase());
        };

        self.operator
            .execute(SetAction::Delete, record, &database, txn)
            .map_err(|err| {
                ProductError::DeleteError("UNION query error:".to_string(), Box::new(err))
            })
    }

    fn insert(
        &mut self,
        _from_port: PortHandle,
        record: &Record,
        txn: &SharedTransaction,
        _reader: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<(SetAction, Record)>, ProductError> {
        let database = if self.db.is_some() {
            self.db
                .map_or(Err(SetError::DatabaseUnavailable), Ok)
                .unwrap()
        } else {
            return Err(ProductError::InvalidDatabase());
        };

        self.operator
            .execute(SetAction::Insert, record, &database, txn)
            .map_err(|err| {
                ProductError::InsertError("UNION query error:".to_string(), Box::new(err))
            })
    }

    #[allow(clippy::type_complexity)]
    fn update(
        &mut self,
        _from_port: PortHandle,
        old: &Record,
        new: &Record,
        txn: &SharedTransaction,
        _reader: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<(Vec<(SetAction, Record)>, Vec<(SetAction, Record)>), ProductError> {
        let database = if self.db.is_some() {
            self.db
                .map_or(Err(SetError::DatabaseUnavailable), Ok)
                .unwrap()
        } else {
            return Err(ProductError::InvalidDatabase());
        };

        let old_records = self
            .operator
            .execute(SetAction::Delete, old, &database, txn)
            .map_err(|err| {
                ProductError::UpdateOldError("UNION query error:".to_string(), Box::new(err))
            })?;

        let new_records = self
            .operator
            .execute(SetAction::Insert, new, &database, txn)
            .map_err(|err| {
                ProductError::UpdateNewError("UNION query error:".to_string(), Box::new(err))
            })?;

        Ok((old_records, new_records))
    }
}

impl Processor for SetProcessor {
    fn init(&mut self, txn: &mut LmdbExclusiveTransaction) -> Result<(), ExecutionError> {
        self.init_store(txn).map_err(|e| InternalError(Box::new(e)))
    }

    fn commit(&self, _epoch: &Epoch, _tx: &SharedTransaction) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn process(
        &mut self,
        from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
        transaction: &SharedTransaction,
        reader: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<(), ExecutionError> {
        match op {
            Operation::Delete { ref old } => {
                let records = self
                    .delete(from_port, old, transaction, reader)
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
                    .insert(from_port, new, transaction, reader)
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
                    .update(from_port, old, new, transaction, reader)
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
