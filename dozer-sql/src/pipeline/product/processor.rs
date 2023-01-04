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
        _from_port: PortHandle,
        record: &Record,
        _txn: &SharedTransaction,
        _reader: &HashMap<PortHandle, RecordReader>,
    ) -> Operation {
        Operation::Delete {
            old: record.clone(),
        }
    }

    fn insert(
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

            if let Some(left_join) = &input_table.left {
                // generate the key with the fields of the left table used in the join contstraint
                let join_key: Vec<u8> = left_join.get_right_record_join_key(record)?;
                // generate the key with theprimary key fields of the left table
                let lookup_key: Vec<u8> = get_lookup_key(record, &input_table.schema)?;
                // Update the Join index
                left_join.update_right_index(&join_key, &lookup_key, database, transaction)?;

                records = left_join.execute_left(
                    records,
                    &join_key,
                    database,
                    transaction,
                    reader,
                    &self.join_tables,
                )?;
            }

            if let Some(right_join) = &input_table.right {
                // generate the key with the fields of the left table used in the join contstraint
                let join_key: Vec<u8> = right_join.get_left_record_join_key(record)?;
                // generate the key with theprimary key fields of the left table
                let lookup_key: Vec<u8> = get_lookup_key(record, &input_table.schema)?;
                // Update the Join index
                right_join.update_left_index(&join_key, &lookup_key, database, transaction)?;

                records = right_join.execute_right(
                    records,
                    &join_key,
                    database,
                    transaction,
                    reader,
                    &self.join_tables,
                )?;
            }

            return Ok(records);
        }

        Err(ExecutionError::InvalidPortHandle(from_port))
    }

    fn update(
        &self,
        _from_port: PortHandle,
        old: &Record,
        new: &Record,
        _txn: &SharedTransaction,
        _reader: &HashMap<PortHandle, RecordReader>,
    ) -> Operation {
        Operation::Update {
            old: old.clone(),
            new: new.clone(),
        }
    }

    // fn merge(&self, _left_records: &[Record], _right_records: &[Record]) -> Vec<Record> {
    //     todo!()
    // }
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
        txn: &SharedTransaction,
        reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<(), ExecutionError> {
        match op {
            Operation::Delete { ref old } => {
                let _ = fw.send(
                    self.delete(from_port, old, txn, reader),
                    DEFAULT_PORT_HANDLE,
                );
            }
            Operation::Insert { ref new } => {
                let records = self.insert(from_port, new, txn, reader)?;

                for record in records.into_iter() {
                    let _ = fw.send(Operation::Insert { new: record }, DEFAULT_PORT_HANDLE);
                }
            }
            Operation::Update { ref old, ref new } => {
                let _ = fw.send(
                    self.update(from_port, old, new, txn, reader),
                    DEFAULT_PORT_HANDLE,
                );
            }
        }
        Ok(())
    }
}
