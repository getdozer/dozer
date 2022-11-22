use dozer_core::dag::node::PortHandle;
use dozer_core::storage::common::{Database, Environment};
use dozer_types::types::Record;
use sqlparser::ast::TableFactor;

use crate::pipeline::errors::PipelineError;

use super::factory::get_input_name;

#[derive(Clone)]
pub struct JoinOperator {
    operator: JoinOperatorType,
    table: PortHandle,
    db: Option<Database>,
}

impl JoinOperator {
    pub fn new(operator: JoinOperatorType, table: PortHandle, txn: &mut dyn Environment) -> Self {
        let mut name = String::from(&table.to_string());
        name.push_str("_right");
        Self {
            operator,
            table,
            db: Some(txn.open_database(&name, false).unwrap()),
        }
    }
}

#[derive(Clone)]
pub struct ReverseJoinOperator {
    operator: JoinOperatorType,
    table: PortHandle,
    db: Option<Database>,
}

impl ReverseJoinOperator {
    pub fn new(operator: JoinOperatorType, table: PortHandle, txn: &mut dyn Environment) -> Self {
        let mut name = String::from(&table.to_string());
        name.push_str("_left");
        Self {
            operator,
            table,
            db: Some(txn.open_database(&name, false).unwrap()),
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
    NaturalJoin,
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
    fn evaluate_left(&self, record: Vec<Record>) -> Result<Vec<Record>, PipelineError>;
    fn evaluate_right(&self, record: Vec<Record>) -> Result<Vec<Record>, PipelineError>;
}

impl JoinExecutor for JoinTable {
    fn evaluate_left(&self, _record: Vec<Record>) -> Result<Vec<Record>, PipelineError> {
        todo!()
    }

    fn evaluate_right(&self, _record: Vec<Record>) -> Result<Vec<Record>, PipelineError> {
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
