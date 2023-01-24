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

use super::join::{get_lookup_key, JoinExecutor, JoinTable};

/// Cartesian Product Processor
#[derive(Debug)]
pub struct FromProcessor {
    /// Join operations
    join_tables: HashMap<PortHandle, JoinTable>,

    /// Database to store Join indexes
    db: Option<Database>,
}

impl FromProcessor {
    /// Creates a new [`FromProcessor`].
    pub fn new(join_tables: HashMap<PortHandle, JoinTable>) -> Self {
        Self {
            join_tables,
            db: None,
        }
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
        // Get the input Table based on the port of the incoming message

        if let Some(input_table) = self.join_tables.get(&from_port) {
            let mut records = vec![record.clone()];

            let database = &self.db.ok_or(ExecutionError::InvalidDatabase)?;

            let mut input_left_join = &input_table.left;

            // Update the Join index
            if let Some(left_join) = input_left_join {
                // generate the key with the fields of the left table used in the join contstraint
                let join_key: Vec<u8> = left_join.get_right_record_join_key(record)?;
                // generate the key with theprimary key fields of the left table
                let lookup_key: Vec<u8> = get_lookup_key(record, &input_table.schema)?;
                // Update the Join index
                left_join.insert_right_index(&join_key, &lookup_key, database, transaction)?;
            }

            while let Some(left_join) = input_left_join {
                let join_key: Vec<u8> = left_join.get_right_record_join_key(record)?;
                records = left_join.execute_left(
                    records,
                    &join_key,
                    database,
                    transaction,
                    reader,
                    &self.join_tables,
                )?;

                let next_table = self.join_tables.get(&left_join.left_table).ok_or(
                    ExecutionError::JoinError(JoinError::InsertPortError(left_join.left_table)),
                )?;
                input_left_join = &next_table.left;
            }

            let mut input_right_join = &input_table.right;

            while let Some(right_join) = input_right_join {
                // generate the key with the fields of the left table used in the join contstraint
                let join_key: Vec<u8> = right_join.get_left_record_join_key(record)?;
                // generate the key with theprimary key fields of the left table
                let lookup_key: Vec<u8> = get_lookup_key(record, &input_table.schema)?;
                // Update the Join index
                right_join.insert_left_index(&join_key, &lookup_key, database, transaction)?;

                records = right_join.execute_right(
                    records,
                    &join_key,
                    database,
                    transaction,
                    reader,
                    &self.join_tables,
                )?;

                let next_table = self.join_tables.get(&right_join.right_table).ok_or(
                    ExecutionError::JoinError(JoinError::InsertPortError(right_join.left_table)),
                )?;
                input_right_join = &next_table.right;
            }

            return Ok(records);
        }

        Err(ExecutionError::JoinError(JoinError::PortNotConnected(
            from_port,
        )))
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
