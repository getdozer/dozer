use crate::pipeline::errors::PipelineError;
use dozer_core::dag::channels::ProcessorChannelForwarder;
use dozer_core::dag::dag::DEFAULT_PORT_HANDLE;
use dozer_core::dag::epoch::Epoch;
use dozer_core::dag::errors::ExecutionError;
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
pub struct ProductProcessor {
    /// Join operations
    join_tables: HashMap<PortHandle, JoinTable>,

    /// Database to store Join indexes
    db: Option<Database>,
}

impl ProductProcessor {
    /// Creates a new [`ProductProcessor`].
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
        from_port: PortHandle,
        record: &Record,
        transaction: &SharedTransaction,
        reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<Vec<Record>, ExecutionError> {
        // Get the input Table based on the port of the incoming message
        if let Some(input_table) = self.join_tables.get(&from_port) {
            let mut records = vec![record.clone()];

            let database = &self.db.ok_or(ExecutionError::InvalidDatabase)?;

            let mut input_left_join = &input_table.left;

            if let Some(left_join) = input_left_join {
                // generate the key with the fields of the left table used in the join contstraint
                let join_key: Vec<u8> = left_join.get_right_record_join_key(record)?;
                // generate the key with theprimary key fields of the left table
                let lookup_key: Vec<u8> = get_lookup_key(record, &input_table.schema)?;
                // Update the Join index
                left_join.delete_right_index(&join_key, &lookup_key, database, transaction)?;
            }

            while let Some(left_join) = input_left_join {
                let join_key: Vec<u8> = left_join.get_right_record_join_key(record)?;
                records = left_join.execute_left(
                    records,
                    &join_table,
                    database,
                    transaction,
                    reader,
                    &self.join_tables,
                )?;

                let next_table = self
                    .join_tables
                    .get(&left_join.left_table)
                    .ok_or(ExecutionError::InvalidPortHandle(left_join.left_table))?;
                input_left_join = &next_table.left;
            }

            let mut input_right_join = &input_table.right;

            while let Some(right_join) = input_right_join {
                // generate the key with the fields of the left table used in the join contstraint
                let join_key: Vec<u8> = right_join.get_left_record_join_key(record)?;
                // generate the key with theprimary key fields of the left table
                let lookup_key: Vec<u8> = get_lookup_key(record, &input_table.schema)?;
                // Update the Join index
                right_join.delete_left_index(&join_key, &lookup_key, database, transaction)?;

                records = right_join.execute_right(
                    records,
                    &join_key,
                    database,
                    transaction,
                    reader,
                    &self.join_tables,
                )?;

                let next_table = self
                    .join_tables
                    .get(&right_join.right_table)
                    .ok_or(ExecutionError::InvalidPortHandle(right_join.left_table))?;
                input_right_join = &next_table.right;
            }

            return Ok(records);
        }

        Err(ExecutionError::InvalidPortHandle(from_port))
    }

    fn insert(
        &self,
        from_port: PortHandle,
        record: &Record,
        transaction: &SharedTransaction,
        reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<Vec<Record>, ExecutionError> {
        // Get the input Table based on the port of the incoming message

        let input_table =
        if let Some(input_table) = self.join_tables.get(&from_port) {
            let mut records = vec![record.clone()];

            let database = &self.db.ok_or(ExecutionError::InvalidDatabase)?;

            let mut input_left_join = &input_table.left;

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

                let next_table = self
                    .join_tables
                    .get(&left_join.left_table)
                    .ok_or(ExecutionError::InvalidPortHandle(left_join.left_table))?;
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

                let next_table = self
                    .join_tables
                    .get(&right_join.right_table)
                    .ok_or(ExecutionError::InvalidPortHandle(right_join.left_table))?;
                input_right_join = &next_table.right;
            }

            return Ok(records);
        }

        Err(ExecutionError::InvalidPortHandle(from_port))
    }

    fn update(
        &self,
        from_port: PortHandle,
        old: &Record,
        new: &Record,
        transaction: &SharedTransaction,
        reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<(Vec<Record>, Vec<Record>), ExecutionError> {
        let input_table = self
            .join_tables
            .get(&from_port)
            .ok_or(ExecutionError::InvalidPortHandle(from_port))?;

        let database = &self.db.ok_or(ExecutionError::InvalidDatabase)?;

        let mut old_records = vec![old.clone()];
        let mut new_records = vec![new.clone()];

        let mut input_left_join = &input_table.left;

        if let Some(left_join) = input_left_join {
            let old_join_key: Vec<u8> = left_join.get_right_record_join_key(old)?;
            let old_lookup_key: Vec<u8> = get_lookup_key(old, &input_table.schema)?;
            let new_join_key: Vec<u8> = left_join.get_right_record_join_key(new)?;
            let new_lookup_key: Vec<u8> = get_lookup_key(new, &input_table.schema)?;

            // Update the Join index
            left_join.delete_right_index(&old_join_key, &old_lookup_key, database, transaction)?;
            left_join.insert_right_index(&new_join_key, &new_lookup_key, database, transaction)?;
        }

        while let Some(left_join) = input_left_join {
            let old_join_key: Vec<u8> = left_join.get_right_record_join_key(old)?;
            old_records = left_join.execute_left(
                old_records,
                &old_join_key,
                database,
                transaction,
                reader,
                &self.join_tables,
            )?;

            let new_join_key: Vec<u8> = left_join.get_right_record_join_key(new)?;
            new_records = left_join.execute_left(
                new_records,
                &new_join_key,
                database,
                transaction,
                reader,
                &self.join_tables,
            )?;

            let next_table = self
                .join_tables
                .get(&left_join.left_table)
                .ok_or(ExecutionError::InvalidPortHandle(left_join.left_table))?;
            input_left_join = &next_table.left;
        }

        let mut input_right_join = &input_table.right;

        if let Some(right_join) = input_right_join {
            let old_join_key: Vec<u8> = right_join.get_left_record_join_key(old)?;
            let old_lookup_key: Vec<u8> = get_lookup_key(old, &input_table.schema)?;
            let new_join_key: Vec<u8> = right_join.get_left_record_join_key(new)?;
            let new_lookup_key: Vec<u8> = get_lookup_key(new, &input_table.schema)?;

            // Update the Join index
            right_join.delete_left_index(&old_join_key, &old_lookup_key, database, transaction)?;
            right_join.insert_left_index(&new_join_key, &new_lookup_key, database, transaction)?;
        }

        while let Some(right_join) = input_right_join {
            let old_join_key: Vec<u8> = right_join.get_left_record_join_key(old)?;

            old_records = right_join.execute_right(
                old_records,
                &old_join_key,
                database,
                transaction,
                reader,
                &self.join_tables,
            )?;

            let new_join_key: Vec<u8> = right_join.get_left_record_join_key(new)?;

            new_records = right_join.execute_right(
                new_records,
                &new_join_key,
                database,
                transaction,
                reader,
                &self.join_tables,
            )?;

            let next_table = self
                .join_tables
                .get(&right_join.right_table)
                .ok_or(ExecutionError::InvalidPortHandle(right_join.left_table))?;
            input_right_join = &next_table.right;
        }

        Ok((old_records, new_records))
    }
}

impl Processor for ProductProcessor {
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
        reader: &HashMap<PortHandle, RecordReader>,
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
