use num_traits::FromPrimitive;

use dozer_types::types::{Field, Record};
use dozer_types::types::Field::Invalid;

use crate::common::error::{DozerSqlError, Result};
use crate::pipeline::expression::expression::{Expression, PhysicalExpression};
use crate::pipeline::expression::comparison::*;
use crate::pipeline::expression::logical::*;
use crate::pipeline::expression::mathematical::*;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum UnaryOperatorType {
    Not,
}

impl UnaryOperatorType {}

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
    Mod
}


impl BinaryOperatorType {
    pub(crate) fn evaluate(&self, left: &Box<Expression>, right: &Box<Expression>, record: &Record) -> Field {
        match self {
            BinaryOperatorType::Eq => evaluate_eq(left, right, record),
            BinaryOperatorType::Ne => evaluate_ne(left, right, record),
            BinaryOperatorType::Gt => evaluate_gt(left, right, record),
            BinaryOperatorType::Gte => evaluate_gte(left, right, record),
            BinaryOperatorType::Lt => evaluate_lt(left, right, record),
            BinaryOperatorType::Lte => evaluate_lte(left, right, record),

            BinaryOperatorType::And => evaluate_and(left, right, record),
            BinaryOperatorType::Or => evaluate_or(left, right, record),

            BinaryOperatorType::Add => evaluate_add(left, right, record),
            BinaryOperatorType::Sub => evaluate_sub(left, right, record),
            BinaryOperatorType::Mul => evaluate_mul(left, right, record),
            BinaryOperatorType::Div => evaluate_div(left, right, record),
            BinaryOperatorType::Mod => evaluate_mod(left, right, record),


            _ => Field::Invalid(format!("Invalid Comparison Operator: {:?}", &self))
        }
    }
}