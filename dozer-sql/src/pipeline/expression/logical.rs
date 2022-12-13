use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use dozer_types::types::{Field, Record};

pub fn evaluate_and(
    left: &Expression,
    right: &Expression,
    record: &Record,
) -> Result<Field, PipelineError> {
    let left_p = left.evaluate(record)?;

    match left_p {
        Field::Boolean(left_v) => {
            if left_p == Field::Boolean(false) {
                return Ok(Field::Boolean(false));
            }
            let right_p = right.evaluate(record)?;
            match right_p {
                Field::Boolean(right_v) => Ok(Field::Boolean(left_v && right_v)),
                _ => Ok(Field::Boolean(false)),
            }
        }
        _ => Err(PipelineError::InvalidOperandType("AND".to_string())),
    }
}

pub fn evaluate_or(
    left: &Expression,
    right: &Expression,
    record: &Record,
) -> Result<Field, PipelineError> {
    let left_p = left.evaluate(record)?;

    match left_p {
        Field::Boolean(left_v) => {
            if left_p == Field::Boolean(true) {
                return Ok(Field::Boolean(true));
            }
            let right_p = right.evaluate(record)?;
            match right_p {
                Field::Boolean(right_v) => Ok(right_p),
                _ => Ok(Field::Boolean(false)),
            }
        }
        _ => Err(PipelineError::InvalidOperandType("OR".to_string())),
    }
}

pub fn evaluate_not(value: &Expression, record: &Record) -> Result<Field, PipelineError> {
    let value_p = value.evaluate(record)?;

    match value_p {
        Field::Boolean(value_v) => Ok(Field::Boolean(!value_v)),
        _ => Err(PipelineError::InvalidOperandType("NOT".to_string())),
    }
}

#[cfg(test)]
use crate::pipeline::expression::execution::Expression::Literal;

#[test]
fn test_bool_bool_and() {
    let row = Record::new(None, vec![]);
    let l = Box::new(Literal(Field::Boolean(true)));
    let r = Box::new(Literal(Field::Boolean(false)));
    assert!(matches!(
        evaluate_and(&l, &r, &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Boolean(false)
    ));
}

#[test]
fn test_bool_bool_or() {
    let row = Record::new(None, vec![]);
    let l = Box::new(Literal(Field::Boolean(true)));
    let r = Box::new(Literal(Field::Boolean(false)));
    assert!(matches!(
        evaluate_or(&l, &r, &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Boolean(true)
    ));
}

#[test]
fn test_bool_not() {
    let row = Record::new(None, vec![]);
    let v = Box::new(Literal(Field::Boolean(true)));
    assert!(matches!(
        evaluate_not(&v, &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Boolean(false)
    ));
}

#[test]
fn test_int_bool_and() {
    let row = Record::new(None, vec![]);
    let l = Box::new(Literal(Field::Int(1)));
    let r = Box::new(Literal(Field::Boolean(true)));
    assert!(evaluate_and(&l, &r, &row).is_err());
}

#[test]
fn test_float_bool_and() {
    let row = Record::new(None, vec![]);
    let l = Box::new(Literal(Field::Float(
        dozer_types::ordered_float::OrderedFloat(1.1),
    )));
    let r = Box::new(Literal(Field::Boolean(true)));
    assert!(evaluate_and(&l, &r, &row).is_err());
}
