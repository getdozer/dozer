use std::collections::HashMap;

use dozer_core::{
    dag::{errors::ExecutionError, node::PortHandle, record_store::RecordReader},
    storage::lmdb_storage::SharedTransaction,
};
use dozer_types::types::Record;
use lmdb::Database;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinOperatorType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinConstraint {
    pub left_key_index: usize,
    pub right_key_index: usize,
}

pub struct JoinOperator {
    operator: JoinOperatorType,

    constraints: Vec<JoinConstraint>,

    left_source: Box<dyn JoinExecutor>,
    right_source: Box<dyn JoinExecutor>,

    // Lookup indexes
    left_index: Database,
    right_index: Database,
}

impl JoinOperator {
    pub fn new(
        operator: JoinOperatorType,
        constraints: Vec<JoinConstraint>,
        left_source: Box<dyn JoinExecutor>,
        right_source: Box<dyn JoinExecutor>,
        left_index: Database,
        right_index: Database,
    ) -> Self {
        Self {
            operator,
            constraints,
            left_source,
            right_source,
            left_index,
            right_index,
        }
    }

    pub fn update_indexes_insert(
        &self,
        from_port: PortHandle,
        record: &Record,
        transaction: &SharedTransaction,
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }

    pub fn insert_index(
        &self,
        from_left: bool,
        key: &[u8],
        value: &[u8],
        transaction: &SharedTransaction,
    ) -> Result<(), ExecutionError> {
        let db = if from_left {
            self.left_index
        } else {
            self.right_index
        };

        let mut exclusive_transaction = transaction.write();
        exclusive_transaction.put(db, key, value)?;

        Ok(())
    }
}

trait JoinExecutor {
    fn insert(
        &self,
        from_port: PortHandle,
        record: &Record,
        lookup_keys: &[(Vec<u8>, u32)],
        transaction: &SharedTransaction,
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<Record>, ExecutionError>;
}

impl JoinExecutor for PortHandle {
    fn insert(
        &self,
        from_port: PortHandle,
        record: &Record,
        lookup_keys: &[(Vec<u8>, u32)],
        transaction: &SharedTransaction,
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<Record>, ExecutionError> {
        let port = self;

        // if the source port is the same as the port of the incoming message, return the record
        if *port == from_port {
            return Ok(vec![*record]);
        }

        let mut result_records = vec![];

        let reader = readers
            .get(&port)
            .ok_or(ExecutionError::InvalidPortHandle(*port))?;

        // retrieve records for the table on the right side of the join
        for (lookup_key, lookup_version) in lookup_keys.iter() {
            if let Some(left_record) = reader.get(lookup_key, *lookup_version)? {
                let join_record = join_records(&mut left_record.clone(), &mut record.clone());
                result_records.push(join_record);
            }
        }

        Ok(result_records)
    }
}

impl JoinExecutor for JoinOperator {
    fn insert(
        &self,
        from_port: PortHandle,
        record: &Record,
        lookup_keys: &[(Vec<u8>, u32)],
        transaction: &SharedTransaction,
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<Record>, ExecutionError> {
        let left_lookup_keys = self.get_lookup_keys(lookup_keys, left_join_index);
        let left_records =
            self.left_source
                .insert(from_port, record, left_lookup_keys, transaction, readers)?;

        let right_lookup_keys = self.get_lookup_keys(lookup_keys, left_join_index);
        let right_records =
            self.right_source
                .insert(from_port, record, right_lookup_keys, transaction, readers);
        let result_records = join(left_records, right_records);
        Ok(result_records)
    }
}

fn join_records(left_record: &mut Record, right_record: &mut Record) -> Record {
    left_record.values.append(&mut right_record.values);
    Record::new(None, left_record.values.clone(), None)
}
