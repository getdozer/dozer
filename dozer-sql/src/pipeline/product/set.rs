use std::fmt::{Display, Formatter};
use std::ptr::read;
use dozer_types::types::{Field, Record, Schema};
use crate::pipeline::errors::{PipelineError, UnsupportedSqlError};
use sqlparser::ast::{Select, SetOperator};
use dozer_types::record_to_map;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SetAction {
    Insert,
    // Delete,
    // Update,
}

#[derive(Clone, PartialEq, Debug)]
pub struct SetOperation {
    pub op: SetOperator,
    pub left: Select,
    pub right: Select,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetConstraint {
    pub left_key_index: usize,
    pub right_key_index: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum SetOperatorType {
    Union,
    UnionAll,
    Except,
    Intersect,
}

impl Display for SetOperatorType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SetOperatorType::Union => f.write_str("UNION"),
            SetOperatorType::UnionAll => f.write_str("UNION ALL"),
            SetOperatorType::Except => f.write_str("EXCEPT"),
            SetOperatorType::Intersect => f.write_str("INTERSECT"),
        }
    }
}

impl SetOperatorType {
    pub fn evaluate(
        &self,
        output_schema: &Schema,
        left: &Expression,
        right: &Expression,
    ) -> Result<Vec<Expression>, PipelineError> {
        match self {
            SetOperatorType::Union => execute_union(output_schema, left, right),
            _ => Err(PipelineError::UnsupportedSqlError(UnsupportedSqlError::GenericError(self.to_string())))
        }
    }
}

pub fn execute_union(
    schema: &Schema,
    left: &Expression,
    right: &Expression,
) -> Result<Vec<Expression>, PipelineError> {

    Ok(vec![Expression::Column{ index: 0 }])
}

