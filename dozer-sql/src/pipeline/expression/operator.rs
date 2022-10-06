use num_traits::FromPrimitive;

use dozer_types::types::{Field, Record};
use dozer_types::types::Field::Invalid;

use crate::common::error::{DozerSqlError, Result};
use crate::pipeline::expression::expression::{Expression, PhysicalExpression};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum UnaryOperatorType {
    Not,
}

impl UnaryOperatorType {}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum BinaryOperatorType {
    // Comparison
    Gte,

    // Mathematical
    Sum,
}


impl BinaryOperatorType {
    pub(crate) fn evaluate(&self, left: &Box<Expression>, right: &Box<Expression>, record: &Record) -> Field {
        match self {
            BinaryOperatorType::Gte => BinaryOperatorType::evaluate_gte(left, right, record),
            _ => Field::Int(999)
        }
    }


    fn evaluate_gte(left: &Box<Expression>, right: &Box<Expression>, record: &Record) -> Field {
        let left_p = left.evaluate(&record);
        let right_p = right.evaluate(&record);

        match left_p {
            Field::Boolean(left_v) => match right_p {
                Field::Boolean(right_v) => Field::Boolean(left_v >= right_v),
                _ => Field::Boolean(false),
            },
            Field::Int(left_v) => match right_p {
                Field::Int(right_v) => Field::Boolean(left_v >= right_v),
                Field::Float(right_v) => {
                    let left_v_f = f64::from_i64(left_v).unwrap();
                    Field::Boolean(left_v_f >= right_v)
                }
                _ => {
                    return Invalid(format!(
                        "Cannot compare int value {} to the current value",
                        left_v
                    ));
                }
            },
            Field::Float(left_v) => match right_p {
                Field::Float(right_v) => Field::Boolean(left_v >= right_v),
                Field::Int(right_v) => {
                    let right_v_f = f64::from_i64(right_v).unwrap();
                    Field::Boolean(left_v >= right_v_f)
                }
                _ => {
                    return Invalid(format!(
                        "Cannot compare float value {} to the current value",
                        left_v
                    ));
                }
            },
            Field::String(left_v) => match right_p {
                Field::String(right_v) => Field::Boolean(left_v >= right_v),
                _ => {
                    return Invalid(format!(
                        "Cannot compare string value {} to the current value",
                        left_v
                    ));
                }
            },
            Field::Timestamp(left_v) => match right_p {
                Field::Timestamp(right_v) => Field::Boolean(left_v >= right_v),
                _ => {
                    return Invalid(format!(
                        "Cannot compare timestamp value {} to the current value",
                        left_v
                    ));
                }
            },
            Field::Binary(left_v) => {
                return Invalid(format!(
                    "Cannot compare binary value to the current value"
                ));
            }
            Field::Invalid(cause) => {
                return Invalid(cause);
            }
            _ => {
                return Invalid(format!("Cannot compare this values"));
            }
        }
    }
}