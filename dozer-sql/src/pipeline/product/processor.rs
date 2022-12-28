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

use super::join::JoinTable;

/// Cartesian Product Processor
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
        _txn: &SharedTransaction,
        _reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<Vec<Record>, ExecutionError> {
        if let Some(input_table) = self.join_tables.get(&from_port) {
            let output_records = vec![record.clone()];

            if let Some(_left_join) = &input_table.left {
                todo!() // left_records = left_join.execute(record, txn, reader)?;
            }

            if let Some(_right_join) = &input_table.right {
                todo!() // right_records = right_join.execute(record, txn, reader)?;
            }
            return Ok(output_records);
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
