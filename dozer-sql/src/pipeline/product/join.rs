use std::collections::HashMap;

use dozer_core::dag::node::PortHandle;
use dozer_core::dag::record_store::RecordReader;
use dozer_core::storage::common::Database;
use dozer_core::storage::errors::StorageError;
use dozer_core::storage::lmdb_storage::SharedTransaction;
use dozer_core::{dag::errors::ExecutionError, storage::prefix_transaction::PrefixTransaction};
use dozer_types::errors::types::TypeError;
use dozer_types::types::{Record, Schema};

use crate::pipeline::expression::builder::NameOrAlias;

const REVERSE_JOIN_FLAG: u32 = 0x80000000;

#[derive(Debug, Clone)]
pub struct JoinTable {
    pub name: NameOrAlias,
    pub schema: Schema,
    pub left: Option<JoinOperator>,
    pub right: Option<JoinOperator>,
}

impl JoinTable {
    pub fn from(name: &NameOrAlias, schema: &Schema) -> Self {
        Self {
            name: name.clone(),
            schema: schema.clone(),
            left: None,
            right: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinOperatorType {
    Inner,
    // LeftOuter,
    // RightOuter,
    // FullOuter,
    // CrossJoin,
    // CrossApply,
    // OuterApply,
}

pub trait JoinExecutor: Send + Sync {
    fn execute_right(
        &self,
        records: Vec<Record>,
        join_key: &[u8],
        database: &Database,
        transaction: &SharedTransaction,
        reader: &HashMap<PortHandle, Box<dyn RecordReader>>,
        join_tables: &HashMap<PortHandle, JoinTable>,
    ) -> Result<Vec<Record>, ExecutionError>;

    fn execute_left(
        &self,
        records: Vec<Record>,
        join_key: &[u8],
        database: &Database,
        transaction: &SharedTransaction,
        reader: &HashMap<PortHandle, Box<dyn RecordReader>>,
        join_tables: &HashMap<PortHandle, JoinTable>,
    ) -> Result<Vec<Record>, ExecutionError>;

    fn insert_right_index(
        &self,
        key: &[u8],
        value: &[u8],
        db: &Database,
        txn: &SharedTransaction,
    ) -> Result<(), ExecutionError>;

    fn insert_left_index(
        &self,
        key: &[u8],
        value: &[u8],
        db: &Database,
        txn: &SharedTransaction,
    ) -> Result<(), ExecutionError>;

    fn delete_right_index(
        &self,
        key: &[u8],
        value: &[u8],
        db: &Database,
        txn: &SharedTransaction,
    ) -> Result<(), ExecutionError>;

    fn delete_left_index(
        &self,
        key: &[u8],
        value: &[u8],
        db: &Database,
        txn: &SharedTransaction,
    ) -> Result<(), ExecutionError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinOperator {
    /// Type of the Join operation
    _operator: JoinOperatorType,

    /// relation on the right side of the JOIN
    pub right_table: PortHandle,

    /// key on the right side of the JOIN
    left_join_key_indexes: Vec<usize>,

    /// relation on the left side of the JOIN
    pub left_table: PortHandle,

    /// key on the left side of the JOIN
    right_join_key_indexes: Vec<usize>,

    /// prefix for the index key
    left_prefix: u32,
}

impl JoinOperator {
    pub fn new(
        _operator: JoinOperatorType,
        right_table: PortHandle,
        left_join_key_indexes: Vec<usize>,
        left_table: PortHandle,
        right_join_key_indexes: Vec<usize>,
    ) -> Self {
        Self {
            _operator,
            right_table,
            left_join_key_indexes,
            left_table,
            right_join_key_indexes,
            left_prefix: (right_table as u32),
        }
    }

    pub fn get_left_record_join_key(&self, record: &Record) -> Result<Vec<u8>, TypeError> {
        get_composite_key(record, self.left_join_key_indexes.as_slice())
    }

    pub fn get_right_record_join_key(&self, record: &Record) -> Result<Vec<u8>, TypeError> {
        get_composite_key(record, self.right_join_key_indexes.as_slice())
    }

    fn get_right_lookup_keys(
        &self,
        join_key: &[u8],
        db: &Database,
        transaction: &SharedTransaction,
    ) -> Result<Vec<(Vec<u8>, u32)>, ExecutionError> {
        let mut join_keys = vec![];

        let mut exclusive_transaction = transaction.write();
        let right_prefix_transaction = PrefixTransaction::new(
            &mut exclusive_transaction,
            self.right_table as u32 | REVERSE_JOIN_FLAG,
        );

        let cursor = right_prefix_transaction.open_cursor(*db)?;

        if !cursor.seek(join_key)? {
            return Ok(join_keys);
        }

        loop {
            let entry = cursor.read()?.ok_or(ExecutionError::InternalDatabaseError(
                StorageError::InvalidRecord,
            ))?;

            if entry.0 != join_key {
                break;
            }

            let (version_bytes, key_bytes) = entry.1.split_at(4);
            let version = u32::from_be_bytes(version_bytes.try_into().unwrap());
            join_keys.push((key_bytes.to_vec(), version));

            if !cursor.next()? {
                break;
            }
        }

        Ok(join_keys)
    }

    fn get_left_lookup_keys(
        &self,
        join_key: &[u8],
        db: &Database,
        transaction: &SharedTransaction,
    ) -> Result<Vec<(Vec<u8>, u32)>, ExecutionError> {
        let mut join_keys = vec![];

        let mut exclusive_transaction = transaction.write();
        let left_prefix_transaction =
            PrefixTransaction::new(&mut exclusive_transaction, self.left_table as u32);

        let cursor = left_prefix_transaction.open_cursor(*db)?;

        if !cursor.seek(join_key)? {
            return Ok(join_keys);
        }

        loop {
            let entry = cursor.read()?.ok_or(ExecutionError::InternalDatabaseError(
                StorageError::InvalidRecord,
            ))?;

            if entry.0 != join_key {
                break;
            }

            let (version_bytes, key_bytes) = entry.1.split_at(4);
            let version = u32::from_be_bytes(version_bytes.try_into().unwrap());
            join_keys.push((key_bytes.to_vec(), version));

            if !cursor.next()? {
                break;
            }
        }

        Ok(join_keys)
    }
}

impl JoinExecutor for JoinOperator {
    fn execute_right(
        &self,
        mut records: Vec<Record>,
        join_key: &[u8],
        db: &Database,
        transaction: &SharedTransaction,
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
        _join_tables: &HashMap<PortHandle, JoinTable>,
    ) -> Result<Vec<Record>, ExecutionError> {
        let mut result_records = vec![];
        let reader = readers
            .get(&self.right_table)
            .ok_or(ExecutionError::InvalidPortHandle(self.right_table))?;
        for record in records.iter_mut() {
            // retrieve the lookup keys for the table on the right side of the join
            let right_lookup_keys = self.get_right_lookup_keys(join_key, db, transaction)?;

            // retrieve records for the table on the right side of the join
            for (right_lookup_key, right_lookup_version) in right_lookup_keys.iter() {
                if let Some(right_record) = reader.get(right_lookup_key, *right_lookup_version)? {
                    let join_record = join_records(&mut record.clone(), &mut right_record.clone());
                    result_records.push(join_record);
                }
            }

            // let join_schema = Schema::empty();

            // let right_table = join_tables.get(&(self.right_table as PortHandle)).ok_or(
            //     ExecutionError::InternalDatabaseError(StorageError::InvalidRecord),
            // )?;

            // if let Some(next_join) = &right_table.right {
            //     let next_join_records = next_join.execute_right(
            //         result_records,
            //         &join_schema,
            //         db,
            //         transaction,
            //         readers,
            //         join_tables,
            //     );
            // }
        }

        Ok(result_records)
    }

    fn execute_left(
        &self,
        mut records: Vec<Record>,
        join_key: &[u8],
        db: &Database,
        transaction: &SharedTransaction,
        readers: &HashMap<PortHandle, Box<dyn RecordReader>>,
        _join_tables: &HashMap<PortHandle, JoinTable>,
    ) -> Result<Vec<Record>, ExecutionError> {
        let mut result_records = vec![];
        let reader = readers
            .get(&self.left_table)
            .ok_or(ExecutionError::InvalidPortHandle(self.left_table))?;

        for record in records.iter_mut() {
            // retrieve the lookup keys for the table on the right side of the join
            let left_lookup_keys = self.get_left_lookup_keys(join_key, db, transaction)?;

            // retrieve records for the table on the right side of the join
            for (left_lookup_key, left_lookup_version) in left_lookup_keys.iter() {
                if let Some(left_record) = reader.get(left_lookup_key, *left_lookup_version)? {
                    let join_record = join_records(&mut left_record.clone(), &mut record.clone());
                    result_records.push(join_record);
                }
            }

            // let join_schema = Schema::empty();

            // let right_table = join_tables.get(&(self.right_table as PortHandle)).ok_or(
            //     ExecutionError::InternalDatabaseError(StorageError::InvalidRecord),
            // )?;

            // if let Some(next_join) = &right_table.right {
            //     let next_join_records = next_join.execute_right(
            //         result_records,
            //         &join_schema,
            //         db,
            //         transaction,
            //         readers,
            //         join_tables,
            //     );
            // }
        }

        Ok(result_records)
    }

    fn insert_right_index(
        &self,
        key: &[u8],
        value: &[u8],
        db: &Database,
        transaction: &SharedTransaction,
    ) -> Result<(), ExecutionError> {
        let mut exclusive_transaction = transaction.write();
        let mut prefix_transaction = PrefixTransaction::new(
            &mut exclusive_transaction,
            self.right_table as u32 | REVERSE_JOIN_FLAG,
        );

        prefix_transaction.put(*db, key, value)?;

        Ok(())
    }

    fn insert_left_index(
        &self,
        key: &[u8],
        value: &[u8],
        db: &Database,
        transaction: &SharedTransaction,
    ) -> Result<(), ExecutionError> {
        let mut exclusive_transaction = transaction.write();
        let mut prefix_transaction =
            PrefixTransaction::new(&mut exclusive_transaction, self.left_table as u32);

        prefix_transaction.put(*db, key, value)?;

        Ok(())
    }

    fn delete_right_index(
        &self,
        key: &[u8],
        value: &[u8],
        db: &Database,
        transaction: &SharedTransaction,
    ) -> Result<(), ExecutionError> {
        let mut exclusive_transaction = transaction.write();
        let mut prefix_transaction = PrefixTransaction::new(
            &mut exclusive_transaction,
            self.right_table as u32 | REVERSE_JOIN_FLAG,
        );

        prefix_transaction.del(*db, key, Some(value))?;

        Ok(())
    }

    fn delete_left_index(
        &self,
        key: &[u8],
        value: &[u8],
        db: &Database,
        transaction: &SharedTransaction,
    ) -> Result<(), ExecutionError> {
        let mut exclusive_transaction = transaction.write();
        let mut prefix_transaction =
            PrefixTransaction::new(&mut exclusive_transaction, self.left_table as u32);

        prefix_transaction.del(*db, key, Some(value))?;

        Ok(())
    }
}

fn join_records(left_record: &mut Record, right_record: &mut Record) -> Record {
    left_record.values.append(&mut right_record.values);
    Record::new(None, left_record.values.clone(), None)
}

pub fn get_composite_key(record: &Record, key_indexes: &[usize]) -> Result<Vec<u8>, TypeError> {
    let mut join_key = Vec::with_capacity(64);

    for key_index in key_indexes.iter() {
        let key_value = record.get_value(*key_index)?;
        let key_bytes = key_value.encode();
        join_key.extend(key_bytes.iter());
    }

    Ok(join_key)
}

pub fn get_lookup_key(record: &Record, schema: &Schema) -> Result<Vec<u8>, TypeError> {
    let mut lookup_key = Vec::with_capacity(64);
    if let Some(version) = record.version {
        lookup_key.extend_from_slice(&version.to_be_bytes());
    } else {
        lookup_key.extend_from_slice(&[0_u8; 4]);
    }

    let key = get_composite_key(record, schema.primary_index.as_slice())?;
    lookup_key.extend(key);
    Ok(lookup_key)
}
