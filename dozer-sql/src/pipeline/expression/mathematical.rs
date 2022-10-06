use num_traits::Bounded;
use num_traits::cast::*;

use dozer_types::types::{Field, Record};
use dozer_types::types::Field::{Boolean, Invalid};

use crate::pipeline::expression::expression::PhysicalExpression;

macro_rules! define_math_oper {
    ($id:ident, $fct:expr, $t: expr) => {
        pub struct $id {
            left: Box<dyn PhysicalExpression>,
            right: Box<dyn PhysicalExpression>,
        }

        impl $id {
            pub fn new(left: Box<dyn PhysicalExpression>, right: Box<dyn PhysicalExpression>) -> Self {
                Self { left, right }
            }
        }

        impl PhysicalExpression for $id {
            fn evaluate(&self, record: &Record) -> Field {
                let left_p = self.left.evaluate(&record);
                let right_p = self.right.evaluate(&record);

                match left_p {
                    Field::Float(left_v) => match right_p {
                        Field::Int(right_v) => {
                            return Field::Float($fct(left_v, f64::from_i64(right_v).unwrap()));
                        }
                        Field::Float(right_v) => {
                            return Field::Float($fct(left_v, right_v));
                        }
                        _ => {
                            return Field::Invalid(
                                "Unable to perform a sum on non-numeric types".to_string(),
                            );
                        }
                    },
                    Field::Int(left_v) => match right_p {
                        Field::Int(right_v) => {
                            return match ($t) {
                                1 => Field::Float($fct(
                                    f64::from_i64(left_v).unwrap(),
                                    f64::from_i64(right_v).unwrap(),
                                )),
                                _ => Field::Int($fct(left_v, right_v)),
                            };
                        }
                        Field::Float(right_v) => {
                            return Field::Float($fct(f64::from_i64(left_v).unwrap(), right_v));
                        }
                        _ => {
                            return Field::Invalid(
                                "Unable to perform a sum on non-numeric types".to_string(),
                            );
                        }
                    },
                    _ => {
                        return Field::Invalid(
                            "Unable to perform a sum on non-numeric types".to_string(),
                        );
                    }
                }
            }
        }
    };
}

define_math_oper!(Add, |a, b| { a + b }, 0);
define_math_oper!(Sub, |a, b| { a - b }, 0);
define_math_oper!(Mul, |a, b| { a * b }, 0);
define_math_oper!(Div, |a, b| { a / b }, 1);
define_math_oper!(Mod, |a, b| { a % b }, 0);

#[test]
fn test_int_int_div() {
    let row = Record::new(None, vec![]);
    let l = Box::new(1);
    let r = Box::new(2);
    let op = Div::new(l, r);
    assert!(matches!(op.evaluate(&row), Field::Float(0.5)));
}

#[test]
fn test_float_int_sum() {
    let row = Record::new(None, vec![]);
    let l = Box::new(1.3);
    let r = Box::new(1);
    let op = Add::new(l, r);
    assert!(matches!(op.evaluate(&row), Field::Float(2.3)));
}

#[test]
fn test_int_int_sum() {
    let row = Record::new(None, vec![]);
    let l = Box::new(1);
    let r = Box::new(1);
    let op = Add::new(l, r);
    assert!(matches!(op.evaluate(&row), Field::Int(2)));
}

#[test]
fn test_int_float_sum() {
    let row = Record::new(None, vec![]);
    let l = Box::new(1.3);
    let r = Box::new(1);
    let op = Add::new(l, r);
    assert!(matches!(op.evaluate(&row), Field::Float(2.3)));
}

#[test]
fn test_composite_sum() {
    let row = Record::new(None, vec![]);
    let ll = Box::new(1);
    let rl = Box::new(1);
    let rr = Box::new(2.5);
    let op = Add::new(ll, Box::new(Add::new(rl, rr)));
    assert!(matches!(op.evaluate(&row), Field::Float(4.5)));
}
