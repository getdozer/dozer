use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::comparison::*;
use crate::pipeline::expression::execution::Expression;
use crate::pipeline::expression::logical::*;
use crate::pipeline::expression::mathematical::*;
use dozer_types::types::{Field, ProcessorRecord, Schema};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum UnaryOperatorType {
    Not,
    Plus,
    Minus,
}

impl Display for UnaryOperatorType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            UnaryOperatorType::Not => f.write_str("!"),
            UnaryOperatorType::Plus => f.write_str("+"),
            UnaryOperatorType::Minus => f.write_str("-"),
        }
    }
}

impl UnaryOperatorType {
    pub fn evaluate(
        &self,
        schema: &Schema,
        value: &Expression,
        record: &ProcessorRecord,
    ) -> Result<Field, PipelineError> {
        match self {
            UnaryOperatorType::Not => evaluate_not(schema, value, record),
            UnaryOperatorType::Plus => evaluate_plus(schema, value, record),
            UnaryOperatorType::Minus => evaluate_minus(schema, value, record),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum BinaryOperatorType {
    // Comparison
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,

    // Logical
    And,
    Or,

    // Mathematical
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

impl Display for BinaryOperatorType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BinaryOperatorType::Eq => f.write_str("="),
            BinaryOperatorType::Ne => f.write_str("!="),
            BinaryOperatorType::Gt => f.write_str(">"),
            BinaryOperatorType::Gte => f.write_str(">="),
            BinaryOperatorType::Lt => f.write_str("<"),
            BinaryOperatorType::Lte => f.write_str("<="),
            BinaryOperatorType::And => f.write_str(" AND "),
            BinaryOperatorType::Or => f.write_str(" OR "),
            BinaryOperatorType::Add => f.write_str("+"),
            BinaryOperatorType::Sub => f.write_str("-"),
            BinaryOperatorType::Mul => f.write_str("*"),
            BinaryOperatorType::Div => f.write_str("/"),
            BinaryOperatorType::Mod => f.write_str("%"),
        }
    }
}

impl BinaryOperatorType {
    pub fn evaluate(
        &self,
        schema: &Schema,
        left: &Expression,
        right: &Expression,
        record: &ProcessorRecord,
    ) -> Result<Field, PipelineError> {
        match self {
            BinaryOperatorType::Eq => evaluate_eq(schema, left, right, record),
            BinaryOperatorType::Ne => evaluate_ne(schema, left, right, record),
            BinaryOperatorType::Gt => evaluate_gt(schema, left, right, record),
            BinaryOperatorType::Gte => evaluate_gte(schema, left, right, record),
            BinaryOperatorType::Lt => evaluate_lt(schema, left, right, record),
            BinaryOperatorType::Lte => evaluate_lte(schema, left, right, record),

            BinaryOperatorType::And => evaluate_and(schema, left, right, record),
            BinaryOperatorType::Or => evaluate_or(schema, left, right, record),

            BinaryOperatorType::Add => evaluate_add(schema, left, right, record),
            BinaryOperatorType::Sub => evaluate_sub(schema, left, right, record),
            BinaryOperatorType::Mul => evaluate_mul(schema, left, right, record),
            BinaryOperatorType::Div => evaluate_div(schema, left, right, record),
            BinaryOperatorType::Mod => evaluate_mod(schema, left, right, record),
        }
    }
}
