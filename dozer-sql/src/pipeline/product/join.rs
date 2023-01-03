use std::collections::HashMap;

use dozer_core::dag::node::PortHandle;
use dozer_core::dag::record_store::RecordReader;
use dozer_core::storage::common::Database;
use dozer_core::storage::errors::StorageError;
use dozer_core::storage::lmdb_storage::SharedTransaction;
use dozer_core::{dag::errors::ExecutionError, storage::prefix_transaction::PrefixTransaction};
use dozer_types::errors::types::TypeError;
use dozer_types::types::{Field, Record, Schema};
use sqlparser::ast::TableFactor;

use crate::pipeline::product::join::StorageError::SerializationError;

use super::factory::get_input_name;

const REVERSE_JOIN_FLAG: u32 = 0x80000000;

#[derive(Debug, Clone)]
pub struct JoinTable {
    pub name: String,
    pub schema: Schema,
    pub left: Option<JoinOperator>,
    pub right: Option<JoinOperator>,
}

impl JoinTable {
    pub fn from(relation: &TableFactor, schema: &Schema) -> Self {
        Self {
            name: get_input_name(relation).unwrap(),
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
        schema: &Schema,
        database: &Database,
        transaction: &SharedTransaction,
        reader: &HashMap<PortHandle, RecordReader>,
        join_tables: &HashMap<PortHandle, JoinTable>,
    ) -> Result<Vec<Record>, ExecutionError>;

    fn execute_left(
        &self,
        records: Vec<Record>,
        schema: &Schema,
        database: &Database,
        transaction: &SharedTransaction,
        reader: &HashMap<PortHandle, RecordReader>,
        join_tables: &HashMap<PortHandle, JoinTable>,
    ) -> Result<Vec<Record>, ExecutionError>;

    fn update_right_index(
        &self,
        key: &[u8],
        value: &[u8],
        db: &Database,
        txn: &SharedTransaction,
    ) -> Result<(), ExecutionError>;

    fn update_left_index(
        &self,
        key: &[u8],
        value: &[u8],
        db: &Database,
        txn: &SharedTransaction,
    ) -> Result<(), ExecutionError>;
}

#[derive(Clone, Debug, PartialEq)]
pub struct JoinOperator {
    /// Type of the Join operation
    _operator: JoinOperatorType,

    /// relation on the right side of the JOIN
    right_table: PortHandle,

    /// key on the right side of the JOIN
    left_join_key_indexes: Vec<usize>,

    /// relation on the left side of the JOIN
    left_table: PortHandle,

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

    pub fn get_record_join_key(&self, record: &Record) -> Result<Vec<u8>, TypeError> {
        get_composite_key(record, self.left_join_key_indexes.as_slice())
    }

    fn get_right_join_keys(
        &self,
        join_key: &[u8],
        db: &Database,
        transaction: &SharedTransaction,
    ) -> Result<Vec<Vec<u8>>, ExecutionError> {
        let mut exclusive_transaction = transaction.write();
        let prefix_transaction = PrefixTransaction::new(
            &mut exclusive_transaction,
            self.right_table as u32 | REVERSE_JOIN_FLAG,
        );

        let cursor = prefix_transaction.open_cursor(*db)?;

        let mut output_keys = vec![];

        if !cursor.seek(join_key)? {
            return Ok(output_keys);
        }

        loop {
            let entry = cursor.read()?.ok_or(ExecutionError::InternalDatabaseError(
                StorageError::InvalidRecord,
            ))?;

            if entry.0 != join_key {
                break;
            }

            if let Some(value) = prefix_transaction.get(*db, entry.1)? {
                // let record = bincode::deserialize(value).map_err(|e| {
                //     StorageError::DeserializationError {
                //         typ: "Schema".to_string(),
                //         reason: Box::new(e),
                //     }
                // })?;
                output_keys.push(value.to_vec());
            } else {
                return Err(ExecutionError::InternalDatabaseError(
                    StorageError::InvalidKey(format!("{:x?}", entry.1)),
                ));
            }

            if !cursor.next()? {
                break;
            }
        }

        Ok(output_keys)
    }

    fn get_left_join_keys(
        &self,
        join_key: &[u8],
        db: &Database,
        transaction: &SharedTransaction,
    ) -> Result<Vec<Vec<u8>>, ExecutionError> {
        let mut exclusive_transaction = transaction.write();
        let prefix_transaction =
            PrefixTransaction::new(&mut exclusive_transaction, self.left_table as u32);

        let cursor = prefix_transaction.open_cursor(*db)?;

        let mut output_keys = vec![];

        if !cursor.seek(join_key)? {
            return Ok(output_keys);
        }

        loop {
            let entry = cursor.read()?.ok_or(ExecutionError::InternalDatabaseError(
                StorageError::InvalidRecord,
            ))?;

            if entry.0 != join_key {
                break;
            }

            if let Some(value) = prefix_transaction.get(*db, entry.1)? {
                // let record = bincode::deserialize(value).map_err(|e| {
                //     StorageError::DeserializationError {
                //         typ: "Schema".to_string(),
                //         reason: Box::new(e),
                //     }
                // })?;
                output_keys.push(value.to_vec());
            } else {
                return Err(ExecutionError::InternalDatabaseError(
                    StorageError::InvalidKey(format!("{:x?}", entry.1)),
                ));
            }

            if !cursor.next()? {
                break;
            }
        }

        Ok(output_keys)
    }
}

impl JoinExecutor for JoinOperator {
    fn execute_right(
        &self,
        mut records: Vec<Record>,
        schema: &Schema,
        db: &Database,
        transaction: &SharedTransaction,
        readers: &HashMap<PortHandle, RecordReader>,
        _join_tables: &HashMap<PortHandle, JoinTable>,
    ) -> Result<Vec<Record>, ExecutionError> {
        let mut result_records = vec![];
        let reader = readers
            .get(&self.right_table)
            .ok_or(ExecutionError::InvalidPortHandle(self.right_table))?;

        for record in records.iter_mut() {
            // generate the key with the fields of the left table used in the join contstraint
            let join_key: Vec<u8> = self.get_record_join_key(record)?;
            // generate the key with theprimary key fields of the left table
            let lookup_key: Vec<u8> = get_lookup_key(record, schema)?;
            self.update_left_index(&join_key, &lookup_key, db, transaction)?;

            // retrieve the lookup keys for the table on the right side of the join
            let right_keys = self.get_right_join_keys(&join_key, db, transaction)?;

            // retrieve records for the table on the right side of the join
            for lookup_key in right_keys.iter() {
                if let Some(record_bytes) = reader.get(lookup_key)? {
                    let right_record: Record =
                        bincode::deserialize(&record_bytes).map_err(|e| SerializationError {
                            typ: "Record".to_string(),
                            reason: Box::new(e),
                        })?;
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
        schema: &Schema,
        db: &Database,
        transaction: &SharedTransaction,
        readers: &HashMap<PortHandle, RecordReader>,
        _join_tables: &HashMap<PortHandle, JoinTable>,
    ) -> Result<Vec<Record>, ExecutionError> {
        let mut result_records = vec![];
        let reader = readers
            .get(&self.left_table)
            .ok_or(ExecutionError::InvalidPortHandle(self.left_table))?;

        for record in records.iter_mut() {
            // generate the key with the fields of the left table used in the join contstraint
            let join_key: Vec<u8> = self.get_record_join_key(record)?;
            // generate the key with theprimary key fields of the left table
            let lookup_key: Vec<u8> = get_lookup_key(record, schema)?;
            self.update_right_index(&join_key, &lookup_key, db, transaction)?;

            // retrieve the lookup keys for the table on the right side of the join
            let left_keys = self.get_left_join_keys(&join_key, db, transaction)?;

            // retrieve records for the table on the right side of the join
            for lookup_key in left_keys.iter() {
                if let Some(record_bytes) = reader.get(lookup_key)? {
                    let left_record: Record =
                        bincode::deserialize(&record_bytes).map_err(|e| SerializationError {
                            typ: "Record".to_string(),
                            reason: Box::new(e),
                        })?;
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

    fn update_right_index(
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

    fn update_left_index(
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
}

fn join_records(left_record: &mut Record, right_record: &mut Record) -> Record {
    left_record.values.append(&mut right_record.values);
    Record::new(None, left_record.values.clone())
}

// impl JoinExecutor for ReverseJoinOperator {
//     fn update_index(
//         &self,
//         key: &[u8],
//         value: &[u8],
//         db: &Database,
//         transaction: &SharedTransaction,
//     ) -> Result<(), ExecutionError> {
//         let mut exclusive_transaction = transaction.write();
//         let mut prefix_transaction =
//             PrefixTransaction::new(&mut exclusive_transaction, self.prefix);

//         prefix_transaction.put(*db, &key, &value)?;

//         Ok(())
//     }

//     fn execute(
//         &self,
//         records: Vec<Record>,
//         schema: &Schema,
//         database: &Database,
//         transaction: &SharedTransaction,
//         readers: &HashMap<PortHandle, RecordReader>,
//         join_tables: &HashMap<PortHandle, JoinTable>,
//     ) -> Result<Vec<Record>, ExecutionError> {
//         let output_records = vec![];

//         // if let Some(_left_reader) = readers.get(&self.left_table) {
//         //     for right_record in records.into_iter() {
//         //         let key = right_record.get_value(self.join_key_indexes[0])?.clone();

//         //         let left_records = get_left_records(db, txn, &self.prefix, &key)?;

//         //         let left_relation_join = join_tables.get(&(self.left_table as PortHandle)).ok_or(
//         //             ExecutionError::InternalDatabaseError(StorageError::InvalidRecord),
//         //         )?;

//         //         if let Some(left_join_op) = &left_relation_join.left {
//         //             let _left_join_records =
//         //                 left_join_op.execute(left_records, db, txn, readers, join_tables);
//         //         }

//         //         //output_records.append(&mut left_records);
//         //     }
//         // }

//         Ok(output_records)
//     }
// }

fn get_left_records(
    db: &Database,
    transaction: &SharedTransaction,
    prefix: &u32,
    key: &Field,
) -> Result<Vec<Record>, ExecutionError> {
    let mut exclusive_transaction = transaction.write();
    let prefix_transaction = PrefixTransaction::new(&mut exclusive_transaction, *prefix);

    let cursor = prefix_transaction.open_cursor(*db)?;

    let mut output_records = vec![];

    let binary_key = key.encode();
    if !cursor.seek(binary_key.as_slice())? {
        return Ok(output_records);
    }

    loop {
        let entry = cursor.read()?.ok_or(ExecutionError::InternalDatabaseError(
            StorageError::InvalidRecord,
        ))?;

        if entry.0 != binary_key {
            break;
        }

        if let Some(value) = prefix_transaction.get(*db, entry.1)? {
            let record =
                bincode::deserialize(value).map_err(|e| StorageError::DeserializationError {
                    typ: "Schema".to_string(),
                    reason: Box::new(e),
                })?;
            output_records.push(record);
        } else {
            return Err(ExecutionError::InternalDatabaseError(
                StorageError::InvalidKey(format!("{:x?}", entry.1)),
            ));
        }

        if !cursor.next()? {
            break;
        }
    }

    Ok(output_records)
}

// pub fn get_from_clause_table_names(
//     from_clause: &[TableWithJoins],
// ) -> Result<Vec<TableName>, PipelineError> {
//     // from_clause
//     //     .into_iter()
//     //     .map(|item| get_table_names(item))
//     //     .flat_map(|result| match result {
//     //         Ok(vec) => vec.into_iter().map(Ok).collect(),
//     //         Err(err) => vec![PipelineError::InvalidExpression(err.to_string())],
//     //     })
//     //     .collect::<Result<Vec<TableName>, PipelineError>>()
//     todo!()
// }

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
    get_composite_key(record, schema.primary_index.as_slice())
}
