use std::collections::HashMap;

use dozer_core::dag::node::PortHandle;
use dozer_core::storage::common::{Database, RwTransaction};
use dozer_core::storage::errors::StorageError;
use dozer_core::storage::record_reader::RecordReader;
use dozer_core::{dag::errors::ExecutionError, storage::prefix_transaction::PrefixTransaction};
use dozer_types::types::{Field, Record};
use sqlparser::ast::TableFactor;

use super::factory::get_input_name;

#[derive(Clone)]
pub struct JoinTable {
    pub name: String,
    pub left: Option<ReverseJoinOperator>,
    pub right: Option<JoinOperator>,
}

impl JoinTable {
    pub fn from(relation: &TableFactor) -> Self {
        Self {
            name: get_input_name(relation).unwrap(),
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
    fn execute(
        &self,
        record: Vec<Record>,
        db: &Database,
        txn: &mut dyn RwTransaction,
        reader: &HashMap<PortHandle, RecordReader>,
        join_tables: &HashMap<PortHandle, JoinTable>,
    ) -> Result<Vec<Record>, ExecutionError>;

    fn update_index(&self, record: &Record, reader: &RecordReader);
}

#[derive(Clone)]
pub struct JoinOperator {
    /// Type of the Join operation
    _operator: JoinOperatorType,

    /// relation on the right side of the JOIN
    right_table: PortHandle,

    /// key on the left side of the JOIN
    _foreign_key_index: usize,

    /// key on the right side of the JOIN
    _primary_key_index: usize,

    /// prefix for the index key
    prefix: u32,
}

impl JoinOperator {
    pub fn new(
        operator: JoinOperatorType,
        right_table: PortHandle,
        foreign_key_index: usize,
        primary_key_index: usize,
        prefix: u32,
    ) -> Self {
        Self {
            _operator: operator,
            right_table,
            _foreign_key_index: foreign_key_index,
            _primary_key_index: primary_key_index,
            prefix,
        }
    }
}

impl JoinExecutor for JoinOperator {
    fn execute(
        &self,
        records: Vec<Record>,
        _db: &Database,
        _txn: &mut dyn RwTransaction,
        readers: &HashMap<PortHandle, RecordReader>,
        _join_tables: &HashMap<PortHandle, JoinTable>,
    ) -> Result<Vec<Record>, ExecutionError> {
        if let Some(reader) = readers.get(&self.right_table) {
            for record in records.iter() {
                self.update_index(record, reader);
            }
        }
        Ok(vec![])
    }

    fn update_index(&self, record: &Record, reader: &RecordReader) {
        todo!()
    }
}

#[derive(Clone)]
pub struct ReverseJoinOperator {
    operator: JoinOperatorType,

    /// relation on the left side of the JOIN
    left_table: PortHandle,

    /// key on the left side of the JOIN
    _foreign_key_index: usize,

    /// key on the right side of the JOIN
    primary_key_index: usize,

    /// prefix for the index key
    prefix: u32,
}

impl ReverseJoinOperator {
    pub fn new(
        operator: JoinOperatorType,
        left_table: PortHandle,
        foreign_key_index: usize,
        primary_key_index: usize,
        prefix: u32,
    ) -> Self {
        Self {
            operator,
            left_table,
            _foreign_key_index: foreign_key_index,
            primary_key_index,
            prefix,
        }
    }
}

impl JoinExecutor for ReverseJoinOperator {
    fn update_index(&self, record: &Record, reader: &RecordReader) {
        todo!()
    }

    fn execute(
        &self,
        records: Vec<Record>,
        db: &Database,
        txn: &mut dyn RwTransaction,
        readers: &HashMap<PortHandle, RecordReader>,
        join_tables: &HashMap<PortHandle, JoinTable>,
    ) -> Result<Vec<Record>, ExecutionError> {
        let mut output_records = vec![];

        if let Some(_left_reader) = readers.get(&self.left_table) {
            for right_record in records.into_iter() {
                let key = right_record.get_value(self.primary_key_index)?.clone();

                let mut left_records = get_left_records(db, txn, &self.prefix, &key)?;

                let left_relation_join = join_tables.get(&(self.left_table as PortHandle)).ok_or(
                    ExecutionError::InternalDatabaseError(StorageError::InvalidRecord),
                )?;

                if let Some(left_join_op) = &left_relation_join.left {
                    let mut left_join_records =
                        left_join_op.execute(left_records, db, txn, readers, join_tables);
                }

                //output_records.append(&mut left_records);
            }
        }

        Ok(output_records)
    }
}

fn get_left_records(
    db: &Database,
    txn: &mut dyn RwTransaction,
    prefix: &u32,
    key: &Field,
) -> Result<Vec<Record>, ExecutionError> {
    let transaction = PrefixTransaction::new(txn, *prefix);

    let cursor = transaction.open_cursor(db)?;

    let mut output_records = vec![];

    let binary_key = key.to_bytes()?;
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

        if let Some(value) = transaction.get(db, entry.1)? {
            let record = bincode::deserialize(value.as_slice()).map_err(|e| {
                StorageError::DeserializationError {
                    typ: "Schema".to_string(),
                    reason: Box::new(e),
                }
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

pub trait IndexUpdater: Send + Sync {
    fn index_insert(
        &self,
        record: &Record,
        txn: &mut dyn RwTransaction,
        reader: &HashMap<PortHandle, RecordReader>,
    );
}

impl IndexUpdater for JoinOperator {
    fn index_insert(
        &self,
        _record: &Record,
        _txn: &mut dyn RwTransaction,
        _reader: &HashMap<PortHandle, RecordReader>,
    ) {
        todo!()
    }
}

impl IndexUpdater for ReverseJoinOperator {
    fn index_insert(
        &self,
        _record: &Record,
        _txn: &mut dyn RwTransaction,
        _reader: &HashMap<PortHandle, RecordReader>,
    ) {
        todo!()
    }
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

// fn get_table_names(item: &TableWithJoins) -> Result<Vec<TableName>, PipelineError> {
//     Err(InvalidExpression("ERR".to_string()))
// }
