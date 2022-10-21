use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use anyhow::anyhow;
use dozer_types::types::{Field, Record};
use num_traits::cast::*;

macro_rules! define_math_operator {
    ($id:ident, $fct:expr, $t: expr) => {
        pub fn $id(
            left: &Expression,
            right: &Expression,
            record: &Record,
        ) -> anyhow::Result<Field> {
            let left_p = left.evaluate(&record)?;
            let right_p = right.evaluate(&record)?;

            match left_p {
                Field::Float(left_v) => match right_p {
                    Field::Int(right_v) => {
                        Ok(Field::Float($fct(left_v, f64::from_i64(right_v).unwrap())))
                    }
                    Field::Float(right_v) => Ok(Field::Float($fct(left_v, right_v))),
                    _ => Err(anyhow!(
                        "Unable to perform a sum on non-numeric types".to_string(),
                    )),
                },
                Field::Int(left_v) => match right_p {
                    Field::Int(right_v) => {
                        return match ($t) {
                            1 => Ok(Field::Float($fct(
                                f64::from_i64(left_v).unwrap(),
                                f64::from_i64(right_v).unwrap(),
                            ))),
                            _ => Ok(Field::Int($fct(left_v, right_v))),
                        };
                    }
                    Field::Float(right_v) => {
                        Ok(Field::Float($fct(f64::from_i64(left_v).unwrap(), right_v)))
                    }
                    _ => Err(anyhow!(
                        "Unable to perform a sum on non-numeric types".to_string(),
                    )),
                },
                _ => Err(anyhow!(
                    "Unable to perform a sum on non-numeric types".to_string(),
                )),
            }
        }
    };
}

define_math_operator!(evaluate_add, |a, b| { a + b }, 0);
define_math_operator!(evaluate_sub, |a, b| { a - b }, 0);
define_math_operator!(evaluate_mul, |a, b| { a * b }, 0);
define_math_operator!(evaluate_div, |a, b| { a / b }, 1);
define_math_operator!(evaluate_mod, |a, b| { a % b }, 0);

pub fn evaluate_plus(expression: &Expression, record: &Record) -> anyhow::Result<Field> {
    let expression_result = expression.evaluate(record)?;

    match expression_result {
        Field::Int(v) => Ok(Field::Int(v)),
        Field::Float(v) => Ok(Field::Float(v)),
        _ => Err(anyhow!(
            "Unary Plus Operator doesn't support non numeric types".to_string()
        )),
    }
}

pub fn evaluate_minus(expression: &Expression, record: &Record) -> anyhow::Result<Field> {
    let expression_result = expression.evaluate(record)?;

    match expression_result {
        Field::Int(v) => Ok(Field::Int(-v)),
        Field::Float(v) => Ok(Field::Float(-v)),
        _ => Err(anyhow!(
            "Unary Minus Operator doesn't support non numeric types".to_string()
        )),
    }
}
