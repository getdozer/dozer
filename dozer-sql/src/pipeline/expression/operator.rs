use crate::pipeline::expression::comparison::*;
use crate::pipeline::expression::execution::Expression;
use crate::pipeline::expression::logical::*;
use crate::pipeline::expression::mathematical::*;
use dozer_types::types::{Field, Record};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum UnaryOperatorType {
    Not,

    Plus,
    Minus,
}

impl UnaryOperatorType {
    pub fn evaluate(&self, value: &Expression, record: &Record) -> anyhow::Result<Field> {
        match self {
            UnaryOperatorType::Not => evaluate_not(value, record),
            UnaryOperatorType::Plus => evaluate_plus(value, record),
            UnaryOperatorType::Minus => evaluate_minus(value, record),
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

impl BinaryOperatorType {
    pub fn evaluate(
        &self,
        left: &Expression,
        right: &Expression,
        record: &Record,
    ) -> anyhow::Result<Field> {
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
        }
    }
}
