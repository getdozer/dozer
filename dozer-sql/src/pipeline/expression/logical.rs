use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use dozer_types::types::{Field, Record};

pub fn evaluate_and(
    left: &Expression,
    right: &Expression,
    record: &Record,
) -> Result<Field, PipelineError> {
    match left.evaluate(record)? {
        Field::Boolean(true) => match right.evaluate(record)? {
            Field::Boolean(true) => Ok(Field::Boolean(true)),
            Field::Boolean(false) => Ok(Field::Boolean(false)),
            Field::Null => Ok(Field::Boolean(false)),
            _ => Err(PipelineError::InvalidOperandType("AND".to_string())),
        },
        Field::Boolean(false) => Ok(Field::Boolean(false)),
        Field::Null => Ok(Field::Boolean(false)),
        _ => Err(PipelineError::InvalidOperandType("AND".to_string())),
    }
}

pub fn evaluate_or(
    left: &Expression,
    right: &Expression,
    record: &Record,
) -> Result<Field, PipelineError> {
    match left.evaluate(record)? {
        Field::Boolean(true) => Ok(Field::Boolean(true)),
        Field::Boolean(false) | Field::Null => match right.evaluate(record)? {
            Field::Boolean(false) => Ok(Field::Boolean(false)),
            Field::Null => Ok(Field::Boolean(false)),
            Field::Boolean(true) => Ok(Field::Boolean(true)),
            _ => Err(PipelineError::InvalidOperandType("OR".to_string())),
        },
        _ => Err(PipelineError::InvalidOperandType("OR".to_string())),
    }
}

pub fn evaluate_not(value: &Expression, record: &Record) -> Result<Field, PipelineError> {
    let value_p = value.evaluate(record)?;

    match value_p {
        Field::Boolean(value_v) => Ok(Field::Boolean(!value_v)),
        Field::Null => Ok(Field::Null),
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
fn test_bool_null_and() {
    let row = Record::new(None, vec![]);
    let l = Box::new(Literal(Field::Boolean(true)));
    let r = Box::new(Literal(Field::Null));
    assert!(matches!(
        evaluate_and(&l, &r, &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Boolean(false)
    ));
}

#[test]
fn test_null_bool_and() {
    let row = Record::new(None, vec![]);
    let l = Box::new(Literal(Field::Null));
    let r = Box::new(Literal(Field::Boolean(true)));
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
fn test_null_bool_or() {
    let row = Record::new(None, vec![]);
    let l = Box::new(Literal(Field::Null));
    let r = Box::new(Literal(Field::Boolean(true)));
    assert!(matches!(
        evaluate_or(&l, &r, &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Boolean(true)
    ));
}

#[test]
fn test_bool_null_or() {
    let row = Record::new(None, vec![]);
    let l = Box::new(Literal(Field::Boolean(true)));
    let r = Box::new(Literal(Field::Null));
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
