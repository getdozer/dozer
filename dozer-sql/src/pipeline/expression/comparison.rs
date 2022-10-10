use num_traits::cast::*;

use dozer_types::types::Field::Invalid;
use dozer_types::types::{Field, Record};

use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};

macro_rules! define_comparison {
    ($id:ident, $function:expr) => {
        pub fn $id(left: &Expression, right: &Expression, record: &Record) -> Field {
            let left_p = left.evaluate(&record);
            let right_p = right.evaluate(&record);

            match left_p {
                Field::Boolean(left_v) => match right_p {
                    Field::Boolean(right_v) => Field::Boolean($function(left_v, right_v)),
                    _ => Field::Boolean(false),
                },
                Field::Int(left_v) => match right_p {
                    Field::Int(right_v) => Field::Boolean($function(left_v, right_v)),
                    Field::Float(right_v) => {
                        let left_v_f = f64::from_i64(left_v).unwrap();
                        Field::Boolean($function(left_v_f, right_v))
                    }
                    _ => {
                        return Invalid(format!(
                            "Cannot compare int value {} to the current value",
                            left_v
                        ));
                    }
                },
                Field::Float(left_v) => match right_p {
                    Field::Float(right_v) => Field::Boolean($function(left_v, right_v)),
                    Field::Int(right_v) => {
                        let right_v_f = f64::from_i64(right_v).unwrap();
                        Field::Boolean($function(left_v, right_v_f))
                    }
                    _ => {
                        return Invalid(format!(
                            "Cannot compare float value {} to the current value",
                            left_v
                        ));
                    }
                },
                Field::String(left_v) => match right_p {
                    Field::String(right_v) => Field::Boolean($function(left_v, right_v)),
                    _ => {
                        return Invalid(format!(
                            "Cannot compare string value {} to the current value",
                            left_v
                        ));
                    }
                },
                Field::Timestamp(left_v) => match right_p {
                    Field::Timestamp(right_v) => Field::Boolean($function(left_v, right_v)),
                    _ => {
                        return Invalid(format!(
                            "Cannot compare timestamp value {} to the current value",
                            left_v
                        ));
                    }
                },
                Field::Binary(_left_v) => {
                    return Invalid(format!("Cannot compare binary value to the current value "));
                }
                Field::Invalid(cause) => {
                    return Invalid(cause);
                }
                _ => {
                    return Invalid(format!("Cannot compare these values"));
                }
            }
        }
    };
}

pub fn evaluate_lt(left: &Expression, right: &Expression, record: &Record) -> Field {
    let left_p = left.evaluate(record);
    let right_p = right.evaluate(record);

    match left_p {
        Field::Boolean(left_v) => match right_p {
            Field::Boolean(right_v) => Field::Boolean(!left_v & right_v),
            _ => Field::Boolean(false),
        },
        Field::Int(left_v) => match right_p {
            Field::Int(right_v) => Field::Boolean(left_v < right_v),
            Field::Float(right_v) => {
                let left_v_f = f64::from_i64(left_v).unwrap();
                Field::Boolean(left_v_f < right_v)
            }
            _ => Invalid(format!(
                "Cannot compare int value {} to the current value",
                left_v
            )),
        },
        Field::Float(left_v) => match right_p {
            Field::Float(right_v) => Field::Boolean(left_v < right_v),
            Field::Int(right_v) => {
                let right_v_f = f64::from_i64(right_v).unwrap();
                Field::Boolean(left_v < right_v_f)
            }
            _ => Invalid(format!(
                "Cannot compare float value {} to the current value",
                left_v
            )),
        },
        Field::String(left_v) => match right_p {
            Field::String(right_v) => Field::Boolean(left_v < right_v),
            _ => Invalid(format!(
                "Cannot compare string value {} to the current value",
                left_v
            )),
        },
        Field::Timestamp(left_v) => match right_p {
            Field::Timestamp(right_v) => Field::Boolean(left_v < right_v),
            _ => Invalid(format!(
                "Cannot compare timestamp value {} to the current value",
                left_v
            )),
        },
        Field::Binary(_left_v) => {
            Invalid("Cannot compare binary value to the current value ".to_string())
        }
        Field::Invalid(cause) => Invalid(cause),
        _ => Invalid("Cannot compare these values".to_string()),
    }
}

pub fn evaluate_gt(left: &Expression, right: &Expression, record: &Record) -> Field {
    let left_p = left.evaluate(record);
    let right_p = right.evaluate(record);

    match left_p {
        Field::Boolean(left_v) => match right_p {
            Field::Boolean(right_v) => Field::Boolean(left_v & !right_v),
            _ => Field::Boolean(false),
        },
        Field::Int(left_v) => match right_p {
            Field::Int(right_v) => Field::Boolean(left_v > right_v),
            Field::Float(right_v) => {
                let left_v_f = f64::from_i64(left_v).unwrap();
                Field::Boolean(left_v_f > right_v)
            }
            _ => Invalid(format!(
                "Cannot compare int value {} to the current value",
                left_v
            )),
        },
        Field::Float(left_v) => match right_p {
            Field::Float(right_v) => Field::Boolean(left_v > right_v),
            Field::Int(right_v) => {
                let right_v_f = f64::from_i64(right_v).unwrap();
                Field::Boolean(left_v > right_v_f)
            }
            _ => Invalid(format!(
                "Cannot compare float value {} to the current value",
                left_v
            )),
        },
        Field::String(left_v) => match right_p {
            Field::String(right_v) => Field::Boolean(left_v > right_v),
            _ => Invalid(format!(
                "Cannot compare string value {} to the current value",
                left_v
            )),
        },
        Field::Timestamp(left_v) => match right_p {
            Field::Timestamp(right_v) => Field::Boolean(left_v > right_v),
            _ => Invalid(format!(
                "Cannot compare timestamp value {} to the current value",
                left_v
            )),
        },
        Field::Binary(_left_v) => {
            Invalid("Cannot compare binary value to the current value ".to_string())
        }
        Field::Invalid(cause) => Invalid(cause),
        _ => Invalid("Cannot compare these values".to_string()),
    }
}

define_comparison!(evaluate_eq, |l, r| { l == r });
define_comparison!(evaluate_ne, |l, r| { l != r });
// define_comparison!(evaluate_lt, |l, r| { l < r });
define_comparison!(evaluate_lte, |l, r| { l <= r });
//define_comparison!(evaluate_gt, |l, r| { l > r });
define_comparison!(evaluate_gte, |l, r| { l >= r });

#[cfg(test)]
use crate::pipeline::expression::execution::Expression::Literal;

#[test]
fn test_float_float_eq() {
    let row = Record::new(None, vec![]);
    let f0 = Box::new(Literal(Field::Float(1.3)));
    let f1 = Box::new(Literal(Field::Float(1.3)));
    assert!(matches!(evaluate_eq(&f0, &f1, &row), Field::Boolean(true)));
}

#[test]
fn test_float_int_eq() {
    let row = Record::new(None, vec![]);
    let f0 = Box::new(Literal(Field::Float(1.0)));
    let f1 = Box::new(Literal(Field::Int(1)));
    assert!(matches!(evaluate_eq(&f0, &f1, &row), Field::Boolean(true)));
}

#[test]
fn test_int_float_eq() {
    let row = Record::new(None, vec![]);
    let f0 = Box::new(Literal(Field::Int(1)));
    let f1 = Box::new(Literal(Field::Float(1.0)));
    assert!(matches!(evaluate_eq(&f0, &f1, &row), Field::Boolean(true)));
}

#[test]
fn test_bool_bool_eq() {
    let row = Record::new(None, vec![]);
    let f0 = Box::new(Literal(Field::Boolean(false)));
    let f1 = Box::new(Literal(Field::Boolean(false)));
    assert!(matches!(evaluate_eq(&f0, &f1, &row), Field::Boolean(true)));
}

#[test]
fn test_str_str_eq() {
    let row = Record::new(None, vec![]);
    let f0 = Box::new(Literal(Field::String("abc".to_string())));
    let f1 = Box::new(Literal(Field::String("abc".to_string())));
    assert!(matches!(evaluate_eq(&f0, &f1, &row), Field::Boolean(true)));
}
