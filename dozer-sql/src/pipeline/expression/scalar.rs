use crate::common::error::{DozerSqlError, Result};
use crate::pipeline::expression::expression::{Expression, PhysicalExpression};
use dozer_types::types::{Field, Record};
use dozer_types::types::Field::Invalid;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum ScalarFunctionType {
    Abs,
    Round,
}

impl ScalarFunctionType {

    pub fn new(name: &str) -> Result<ScalarFunctionType> {
        Ok(match name {
            "abs" => ScalarFunctionType::Abs,
            "round" => ScalarFunctionType::Round,
            _ => {
                return Err(DozerSqlError::NotImplemented(format!(
                    "Unsupported Scalar function: {}",
                    name
                )));
            }
        })
    }

    pub(crate) fn evaluate(&self, args: &Vec<Box<Expression>>, record: &Record) -> Field {
        match self {
            ScalarFunctionType::Abs => ScalarFunctionType::evaluate_abs(&args[0], record),
            ScalarFunctionType::Round => ScalarFunctionType::evaluate_round(&args[0], record),
            _ => Field::Int(999)
        }
    }

    fn evaluate_abs(arg: &Box<Expression>, record: &Record) -> Field {
        let value = arg.evaluate(record);
        match value {
            Field::Int(i) => Field::Int(i.abs()),
            Field::Float(f) => Field::Float(f.abs()),
            _ => Field::Int(998)
        }

    }

    fn evaluate_round(arg: &Box<Expression>, record: &Record) -> Field {
        let value = arg.evaluate(record);
        match value {
            Field::Int(i) => Field::Int(i),
            Field::Float(f) => Field::Float((f * 100.0).round() / 100.0),
            _ => Field::Int(998)
        }
    }
}