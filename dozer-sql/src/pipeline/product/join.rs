use std::collections::HashMap;

use dozer_core::dag::errors::ExecutionError;
use dozer_core::dag::node::PortHandle;
use dozer_core::dag::record_store::RecordReader;
use dozer_core::storage::common::{Environment, RwTransaction};
use dozer_types::types::Record;
use sqlparser::ast::TableFactor;

use crate::pipeline::errors::PipelineError;

use super::factory::get_input_name;

#[derive(Clone)]
pub struct JoinOperator {
    /// Type of the Join operation
    operator: JoinOperatorType,

    /// relation on the right side of the JOIN
    table: PortHandle,

    /// key on the left side of the JOIN
    foreign_key_index: usize,

    /// key on the right side of the JOIN
    primary_key_index: usize,

    /// prefix for the index key
    prefix: String,
}

impl JoinOperator {
    pub fn new(
        operator: JoinOperatorType,
        table: PortHandle,
        foreign_key_index: usize,
        primary_key_index: usize,
    ) -> Self {
        let mut prefix = table.to_string();
        prefix.push_str("r");
        Self {
            operator,
            table,
            foreign_key_index,
            primary_key_index,
            prefix,
        }
    }
}

#[derive(Clone)]
pub struct ReverseJoinOperator {
    operator: JoinOperatorType,

    /// relation on the left side of the JOIN
    table: PortHandle,

    /// key on the left side of the JOIN
    foreign_key_index: usize,

    /// key on the right side of the JOIN
    primary_key_index: usize,

    /// prefix for the index key
    prefix: String,
}

impl ReverseJoinOperator {
    pub fn new(
        operator: JoinOperatorType,
        table: PortHandle,
        foreign_key_index: usize,
        primary_key_index: usize,
    ) -> Self {
        let mut prefix = table.to_string();
        prefix.push_str("l");
        Self {
            operator,
            table,
            foreign_key_index,
            primary_key_index,
            prefix,
        }
    }
}

#[derive(Clone)]
pub struct JoinTable {
    pub name: String,
    pub left: Option<ReverseJoinOperator>,
    pub right: Option<JoinOperator>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinOperatorType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    CrossJoin,
    CrossApply,
    OuterApply,
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

pub trait JoinExecutor: Send + Sync {
    fn execute(
        &self,
        record: &Record,
        txn: &mut dyn RwTransaction,
        reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<Vec<Record>, ExecutionError>;
}

impl JoinExecutor for JoinOperator {
    fn execute(
        &self,
        record: &Record,
        txn: &mut dyn RwTransaction,
        reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<Vec<Record>, ExecutionError> {
        todo!()
    }
}

impl JoinExecutor for ReverseJoinOperator {
    fn execute(
        &self,
        record: &Record,
        txn: &mut dyn RwTransaction,
        reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<Vec<Record>, ExecutionError> {
        todo!()
    }
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
        record: &Record,
        _txn: &mut dyn RwTransaction,
        _reader: &HashMap<PortHandle, RecordReader>,
    ) {
        todo!()
    }
}

// impl JoinOperatorType {
//     pub fn execute(
//         &self,
//         left: &[Record],
//         right: &[Record],
//         contraint: &Expression,
//     ) -> Result<Record, PipelineError> {
//         let (record, schema) = match self {
//             JoinOperatorType::NaturalJoin => execute_natural_join(left, right),
//         };
//         execute_selection(expression, record)
//     }
// }

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
