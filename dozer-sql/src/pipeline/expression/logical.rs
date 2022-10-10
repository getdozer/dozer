use dozer_types::types::{Field, Record};
use dozer_types::types::Field::Invalid;

use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};

pub fn evaluate_and(left: &Expression, right: &Expression, record: &Record) -> Field {
    let left_p = left.evaluate(record);

    match left_p {
        Field::Boolean(left_v) => {
            if left_p == Field::Boolean(false) {
                return Field::Boolean(false);
            }
            let right_p = right.evaluate(record);
            match right_p {
                Field::Boolean(right_v) => Field::Boolean(left_v && right_v),
                _ => Field::Boolean(false),
            }
        }
        _ => {
            Invalid(format!("Cannot apply {} to this values", "$id"))
        }
    }
}

pub fn evaluate_or(left: &Expression, right: &Expression, record: &Record) -> Field {
    let left_p = left.evaluate(record);

    match left_p {
        Field::Boolean(left_v) => {
            if left_p == Field::Boolean(true) {
                return Field::Boolean(true);
            }
            let right_p = right.evaluate(record);
            match right_p {
                Field::Boolean(right_v) => Field::Boolean(left_v && right_v),
                _ => Field::Boolean(false),
            }
        }
        _ => {
            Invalid(format!("Cannot apply {} to this values", "$id"))
        }
    }
}

pub fn evaluate_not(value: &Expression, record: &Record) -> Field {
    let value_p = value.evaluate(record);

    match value_p {
        Field::Boolean(value_v) => Field::Boolean(!value_v),
        _ => {
            Invalid(format!("Cannot apply {} to this values", "$id"))
        }
    }
}

#[cfg(test)]
use crate::pipeline::expression::execution::Expression::Literal;

#[test]
fn test_bool_bool_and() {
    let row = Record::new(None, vec![]);
    let l = Box::new(Literal(Field::Boolean(true)));
    let r = Box::new(Literal(Field::Boolean(false)));
    assert!(matches!(evaluate_and(&l, &r, &row), Field::Boolean(false)));
}

#[test]
fn test_bool_bool_or() {
    let row = Record::new(None, vec![]);
    let l = Box::new(Literal(Field::Boolean(true)));
    let r = Box::new(Literal(Field::Boolean(false)));
    assert!(matches!(evaluate_or(&l, &r, &row), Field::Boolean(true)));
}

#[test]
fn test_bool_not() {
    let row = Record::new(None, vec![]);
    let v = Box::new(Literal(Field::Boolean(true)));
    assert!(matches!(evaluate_not(&v, &row), Field::Boolean(false)));
}

#[test]
fn test_int_bool_and() {
    let row = Record::new(None, vec![]);
    let l = Box::new(Literal(Field::Int(1)));
    let r = Box::new(Literal(Field::Boolean(true)));
    assert!(matches!(evaluate_and(&l, &r, &row), Invalid(_)));
}

#[test]
fn test_float_bool_and() {
    let row = Record::new(None, vec![]);
    let l = Box::new(Literal(Field::Float(1.1)));
    let r = Box::new(Literal(Field::Boolean(true)));
    assert!(matches!(evaluate_and(&l, &r, &row), Invalid(_)));
}