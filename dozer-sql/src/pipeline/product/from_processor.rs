use crate::pipeline::errors::PipelineError;
use dozer_core::dag::channels::ProcessorChannelForwarder;
use dozer_core::dag::dag::DEFAULT_PORT_HANDLE;
use dozer_core::dag::epoch::Epoch;
use dozer_core::dag::errors::{ExecutionError, JoinError};
use dozer_core::dag::node::{PortHandle, Processor};
use dozer_core::dag::record_store::RecordReader;
use dozer_core::storage::common::Database;
use dozer_core::storage::lmdb_storage::{LmdbEnvironmentManager, SharedTransaction};
use dozer_types::internal_err;
use dozer_types::types::{Operation, Record};
use std::collections::HashMap;

use dozer_core::dag::errors::ExecutionError::InternalError;

use super::from_join::JoinOperator;
use super::join::{get_lookup_key, JoinExecutor, JoinTable};

/// Cartesian Product Processor
#[derive(Debug)]
pub struct FromProcessor {
    /// Join operations
    operator: JoinOperator,

    /// Database to store Join indexes
    db: Option<Database>,
}

impl FromProcessor {
    /// Creates a new [`FromProcessor`].
    pub fn new(operator: JoinOperator) -> Self {
        Self { operator, db: None }
    }

    fn init_store(&mut self, env: &mut LmdbEnvironmentManager) -> Result<(), PipelineError> {
        self.db = Some(env.open_database("product", true)?);

        Ok(())
    }

    fn delete(
        &self,
        _from_port: PortHandle,
        _record: &Record,
        _transaction: &SharedTransaction,
        _reader: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<Record>, ExecutionError> {
        todo!()
    }

    fn insert(
        &self,
        from_port: PortHandle,
        record: &Record,
        transaction: &SharedTransaction,
        reader: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<Record>, ExecutionError> {
        let database = &self.db.ok_or(ExecutionError::InvalidDatabase)?;

        let lookup_keys = vec![];
        return self.operator.insert(
            from_port,
            record,
            &lookup_keys,
            database,
            transaction,
            reader,
        );
    }

    fn update(
        &self,
        _from_port: PortHandle,
        _old: &Record,
        _new: &Record,
        _transaction: &SharedTransaction,
        _reader: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<(Vec<Record>, Vec<Record>), ExecutionError> {
        todo!()
    }
}

impl Processor for FromProcessor {
    fn init(&mut self, state: &mut LmdbEnvironmentManager) -> Result<(), ExecutionError> {
        internal_err!(self.init_store(state))
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
                let records = self.delete(from_port, old, transaction, reader)?;

                for record in records.into_iter() {
                    let _ = fw.send(Operation::Delete { old: record }, DEFAULT_PORT_HANDLE);
                }
            }
            Operation::Insert { ref new } => {
                let records = self.insert(from_port, new, transaction, reader)?;

                for record in records.into_iter() {
                    let _ = fw.send(Operation::Insert { new: record }, DEFAULT_PORT_HANDLE);
                }
            }
            Operation::Update { ref old, ref new } => {
                let (old_join_records, new_join_records) =
                    self.update(from_port, old, new, transaction, reader)?;

                for old in old_join_records.into_iter() {
                    let _ = fw.send(Operation::Delete { old }, DEFAULT_PORT_HANDLE);
                }

                for new in new_join_records.into_iter() {
                    let _ = fw.send(Operation::Insert { new }, DEFAULT_PORT_HANDLE);
                }
            }
        }
        Ok(())
    }
}
