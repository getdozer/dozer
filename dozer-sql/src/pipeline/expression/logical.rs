use crate::pipeline::expression::expression::{PhysicalExpression, Timestamp};
use dozer_types::types::Field::{Boolean, Invalid};
use dozer_types::types::{Field, Record};
use num_traits::cast::*;
use num_traits::Bounded;

pub struct And {
    left: Box<dyn PhysicalExpression>,
    right: Box<dyn PhysicalExpression>,
}

impl And {
    pub fn new(left: Box<dyn PhysicalExpression>, right: Box<dyn PhysicalExpression>) -> Self {
        Self { left, right }
    }
}

impl PhysicalExpression for And {
    fn evaluate(&self, record: &Record) -> Field {
        let left_p = self.left.evaluate(&record);

        match left_p {
            Field::Boolean(left_v) => {
                if left_p == Field::Boolean(false) {
                    return Field::Boolean(false);
                }
                let right_p = self.right.evaluate(&record);
                match right_p {
                    Field::Boolean(right_v) => Field::Boolean(left_v && right_v),
                    _ => Field::Boolean(false),
                }
            }
            _ => {
                return Invalid(format!("Cannot apply {} to this values", "$id".to_string()));
            }
        }
    }
}

pub struct Or {
    left: Box<dyn PhysicalExpression>,
    right: Box<dyn PhysicalExpression>,
}

impl Or {
    pub fn new(left: Box<dyn PhysicalExpression>, right: Box<dyn PhysicalExpression>) -> Self {
        Self { left, right }
    }
}

impl PhysicalExpression for Or {
    fn evaluate(&self, record: &Record) -> Field {
        let left_p = self.left.evaluate(&record);

        match left_p {
            Field::Boolean(left_v) => {
                if left_p == Field::Boolean(true) {
                    return Field::Boolean(true);
                }
                let right_p = self.right.evaluate(&record);
                match right_p {
                    Field::Boolean(right_v) => Field::Boolean(left_v && right_v),
                    _ => Field::Boolean(false),
                }
            }
            _ => {
                return Invalid(format!("Cannot apply {} to this values", "$id".to_string()));
            }
        }
    }
}

pub struct Not {
    value: Box<dyn PhysicalExpression>,
}

impl Not {
    pub fn new(value: Box<dyn PhysicalExpression>) -> Self {
        Self { value }
    }
}

impl PhysicalExpression for Not {
    fn evaluate(&self, record: &Record) -> Field {
        let value_p = self.value.evaluate(&record);

        match value_p {
            Field::Boolean(value_v) => Field::Boolean(!value_v),
            _ => {
                return Invalid(format!("Cannot apply {} to this values", "$id".to_string()));
            }
        }
    }
}

#[test]
fn test_bool_bool_and() {
    let row = Record::new(None, vec![]);
    let l = Box::new(true);
    let r = Box::new(false);
    let op = And::new(l, r);
    assert!(matches!(op.evaluate(&row), Field::Boolean(false)));
}

#[test]
fn test_bool_bool_or() {
    let row = Record::new(None, vec![]);
    let l = Box::new(true);
    let r = Box::new(false);
    let op = Or::new(l, r);
    assert!(matches!(op.evaluate(&row), Field::Boolean(true)));
}

#[test]
fn test_bool_not() {
    let row = Record::new(None, vec![]);
    let v = Box::new(true);
    let op = Not::new(v);
    assert!(matches!(op.evaluate(&row), Field::Boolean(false)));
}

#[test]
fn test_int_bool_and() {
    let row = Record::new(None, vec![]);
    let l = Box::new(1);
    let r = Box::new(true);
    let op = And::new(l, r);
    assert!(matches!(op.evaluate(&row), Invalid(_)));
}
