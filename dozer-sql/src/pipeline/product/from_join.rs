use std::collections::HashMap;

use dozer_core::{
    dag::{errors::ExecutionError, node::PortHandle, record_store::RecordReader},
    storage::{lmdb_storage::SharedTransaction, prefix_transaction::PrefixTransaction},
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

#[derive(Clone, Debug)]
pub enum JoinSource {
    Table(PortHandle),
    Join(JoinOperator),
}

impl JoinSource {
    fn insert(
        &self,
        from_port: PortHandle,
        record: &Record,
        lookup_keys: &[(Vec<u8>, u32)],
        transaction: &SharedTransaction,
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<Record>, ExecutionError> {
        match self {
            JoinSource::Table(port) => {
                let reader = readers
                    .get(port)
                    .ok_or(ExecutionError::InvalidPortHandle(*port))?;
                let mut result_records = vec![];
                for (lookup_key, lookup_version) in lookup_keys.iter() {
                    if let Some(left_record) = reader.get(lookup_key, *lookup_version)? {
                        let join_record =
                            join_records(&mut left_record.clone(), &mut record.clone());
                        result_records.push(join_record);
                    }
                }
                return Ok(result_records);
            }
            JoinSource::Join(join) => {
                return join.insert(from_port, record, lookup_keys, transaction, readers);
            }
        }

        // let port = self;

        // // if the source port is the same as the port of the incoming message, return the record
        // if *port == from_port {
        //     return Ok(vec![*record]);
        // }

        // let mut result_records = vec![];

        // let reader = readers
        //     .get(&port)
        //     .ok_or(ExecutionError::InvalidPortHandle(*port))?;

        // // retrieve records for the table on the right side of the join
        // for (lookup_key, lookup_version) in lookup_keys.iter() {
        //     if let Some(left_record) = reader.get(lookup_key, *lookup_version)? {
        //         let join_record = join_records(&mut left_record.clone(), &mut record.clone());
        //         result_records.push(join_record);
        //     }
        // }

        // Ok(result_records)
    }
}

#[derive(Clone, Debug)]
pub struct JoinOperator {
    operator: JoinOperatorType,

    constraints: Vec<JoinConstraint>,

    left_source: Box<JoinSource>,
    right_source: Box<JoinSource>,

    // Lookup indexes
    left_index: u32,
    right_index: u32,
}

impl JoinOperator {
    pub fn new(
        operator: JoinOperatorType,
        constraints: Vec<JoinConstraint>,
        left_source: Box<JoinSource>,
        right_source: Box<JoinSource>,
        left_index: u32,
        right_index: u32,
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

    pub fn insert(
        &self,
        from_port: PortHandle,
        record: &Record,
        lookup_keys: &[(Vec<u8>, u32)],
        transaction: &SharedTransaction,
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<Vec<Record>, ExecutionError> {
        let left_lookup_keys = vec![]; // self.get_lookup_keys(lookup_keys, left_join_index);
        let left_records =
            self.left_source
                .insert(from_port, record, &left_lookup_keys, transaction, readers)?;

        let right_lookup_keys = vec![]; // self.get_lookup_keys(lookup_keys, left_join_index);
        let right_records =
            self.right_source
                .insert(from_port, record, &right_lookup_keys, transaction, readers);
        let result_records = vec![]; //join(left_records, right_records);
        Ok(result_records)
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
        db: &Database,
        transaction: &SharedTransaction,
    ) -> Result<(), ExecutionError> {
        let prefix = if from_left {
            self.left_index
        } else {
            self.right_index
        };

        let mut exclusive_transaction = transaction.write();
        let mut prefix_transaction = PrefixTransaction::new(&mut exclusive_transaction, prefix);

        prefix_transaction.put(*db, key, value)?;

        Ok(())
    }
}

fn join_records(left_record: &mut Record, right_record: &mut Record) -> Record {
    left_record.values.append(&mut right_record.values);
    Record::new(None, left_record.values.clone(), None)
}
