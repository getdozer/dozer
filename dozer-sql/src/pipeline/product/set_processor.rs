use std::collections::HashMap;
use lmdb::{Database, DatabaseFlags};
use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_core::epoch::Epoch;
use dozer_core::errors::ExecutionError;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::record_store::RecordReader;
use dozer_core::storage::lmdb_storage::{LmdbExclusiveTransaction, SharedTransaction};
use dozer_types::internal_err;
use dozer_types::types::{Operation, Record};
use crate::pipeline::errors::{PipelineError, ProductError};
use crate::pipeline::product::set::{SetAction, SetOperation};
use dozer_core::errors::ExecutionError::InternalError;
use dozer_core::storage::prefix_transaction::PrefixTransaction;
use dozer_types::parking_lot::RwLockWriteGuard;

const SET_PREFIX: u32 = 100000;

#[derive(Debug)]
pub struct SetProcessor {
    /// Set operations
    operator: SetOperation,
    source_names: HashMap<PortHandle, String>,
    /// Database to store Join indexes
    db: Option<Database>,
}

impl SetProcessor {
    /// Creates a new [`FromProcessor`].
    pub fn new(operator: SetOperation, source_names: HashMap<PortHandle, String>) -> Self {
        Self {
            operator,
            source_names,
            db: None,
        }
    }

    fn init_store(&mut self, txn: &mut LmdbExclusiveTransaction) -> Result<(), PipelineError> {
        self.db = Some(txn.create_database(Some("set"), Some(DatabaseFlags::DUP_SORT))?);

        Ok(())
    }

    fn delete(
        &mut self,
        from_port: PortHandle,
        record: &Record,
        txn: &mut PrefixTransaction,
        _reader: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<(SetAction, Record)>, ProductError> {
        let database = if self.db.is_some() {
            self.db.unwrap()
        } else {
            return Err(ProductError::InvalidDatabase());
        };

        self.operator
            .execute(
                SetAction::Delete,
                record,
                &database,
                txn,
            )
            .map_err(|err| ProductError::DeleteError(self.get_port_name(from_port), Box::new(err)))
    }

    fn insert(
        &mut self,
        from_port: PortHandle,
        record: &Record,
        txn: &mut PrefixTransaction,
        _reader: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<(SetAction, Record)>, ProductError> {
        let database = if self.db.is_some() {
            self.db.unwrap()
        } else {
            return Err(ProductError::InvalidDatabase());
        };

        self.operator
            .execute(
                SetAction::Insert,
                record,
                &database,
                txn,
            )
            .map_err(|err| ProductError::InsertError(self.get_port_name(from_port), Box::new(err)))
    }

    #[allow(clippy::type_complexity)]
    fn update(
        &mut self,
        from_port: PortHandle,
        old: &Record,
        new: &Record,
        txn: &mut PrefixTransaction,
        _reader: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<(Vec<(SetAction, Record)>, Vec<(SetAction, Record)>), ProductError> {
        let database = if self.db.is_some() {
            self.db.unwrap()
        } else {
            return Err(ProductError::InvalidDatabase());
        };

        let old_records = self
            .operator
            .execute(
                SetAction::Delete,
                old,
                &database,
                txn,
            )
            .map_err(|err| {
                ProductError::UpdateOldError(self.get_port_name(from_port), Box::new(err))
            })?;

        let new_records = self
            .operator
            .execute(
                SetAction::Insert,
                new,
                &database,
                txn,
            )
            .map_err(|err| {
                ProductError::UpdateNewError(self.get_port_name(from_port), Box::new(err))
            })?;

        Ok((old_records, new_records))
    }

    fn get_port_name(&self, from_port: u16) -> String {
        self.source_names
            .get(&from_port)
            .unwrap_or(&from_port.to_string())
            .to_string()
    }
}

impl Processor for SetProcessor {
    fn init(&mut self, txn: &mut LmdbExclusiveTransaction) -> Result<(), ExecutionError> {
        internal_err!(self.init_store(txn))
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
        let lmdb_txn = &mut transaction.write();
        let ptx = &mut PrefixTransaction::new(lmdb_txn, SET_PREFIX);
        match op {
            Operation::Delete { ref old } => {
                let records = self
                    .delete(from_port, old, ptx, reader)
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
                    .insert(from_port, new, ptx, reader)
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
                    .update(from_port, old, new, ptx, reader)
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
