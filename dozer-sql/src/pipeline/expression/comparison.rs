use crate::pipeline::expression::expression::{PhysicalExpression, Timestamp};
use dozer_types::types::Field::{Boolean, Invalid};
use dozer_types::types::{Field, Record};
use num_traits::cast::*;
use num_traits::Bounded;

macro_rules! define_cmp_oper {
    ($id:ident, $fct:expr) => {
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
                    Field::Boolean(left_v) => match right_p {
                        Field::Boolean(right_v) => Field::Boolean($fct(left_v, right_v)),
                        _ => Field::Boolean(false),
                    },
                    Field::Int(left_v) => match right_p {
                        Field::Int(right_v) => Field::Boolean($fct(left_v, right_v)),
                        Field::Float(right_v) => {
                            let left_v_f = f64::from_i64(left_v).unwrap();
                            Field::Boolean($fct(left_v_f, right_v))
                        }
                        _ => {
                            return Invalid(format!(
                                "Cannot compare int value {} to the current value",
                                left_v
                            ));
                        }
                    },
                    Field::Float(left_v) => match right_p {
                        Field::Float(right_v) => Field::Boolean($fct(left_v, right_v)),
                        Field::Int(right_v) => {
                            let right_v_f = f64::from_i64(right_v).unwrap();
                            Field::Boolean($fct(left_v, right_v_f))
                        }
                        _ => {
                            return Invalid(format!(
                                "Cannot compare float value {} to the current value",
                                left_v
                            ));
                        }
                    },
                    Field::String(left_v) => match right_p {
                        Field::String(right_v) => Field::Boolean($fct(left_v, right_v)),
                        _ => {
                            return Invalid(format!(
                                "Cannot compare string value {} to the current value",
                                left_v
                            ));
                        }
                    },
                    Field::Timestamp(left_v) => match right_p {
                        Field::Timestamp(right_v) => Field::Boolean($fct(left_v, right_v)),
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
    };
}

define_cmp_oper!(Eq, |l, r| { l == r });
define_cmp_oper!(Ne, |l, r| { l != r });
define_cmp_oper!(Lt, |l, r| { l < r });
define_cmp_oper!(Lte, |l, r| { l <= r });
define_cmp_oper!(Gt, |l, r| { l > r });
define_cmp_oper!(Gte, |l, r| { l >= r });

#[test]
fn test_float_float_eq() {
    let row = Record::new(None, vec![]);
    let f0 = Box::new(1.3);
    let f1 = Box::new(1.3);
    let eq = Eq::new(f0, f1);
    assert!(matches!(eq.evaluate(&row), Field::Boolean(true)));
}

#[test]
fn test_float_int_eq() {
    let row = Record::new(None, vec![]);
    let f0 = Box::new(1.0);
    let f1 = Box::new(1);
    let eq = Eq::new(f0, f1);
    assert!(matches!(eq.evaluate(&row), Field::Boolean(true)));
}

#[test]
fn test_int_float_eq() {
    let row = Record::new(None, vec![]);
    let f0 = Box::new(1);
    let f1 = Box::new(1.0);
    let eq = Eq::new(f0, f1);
    assert!(matches!(eq.evaluate(&row), Field::Boolean(true)));
}

#[test]
fn test_bool_bool_eq() {
    let row = Record::new(None, vec![]);
    let f0 = Box::new(false);
    let f1 = Box::new(false);
    let eq = Eq::new(f0, f1);
    assert!(matches!(eq.evaluate(&row), Field::Boolean(true)));
}

#[test]
fn test_str_str_eq() {
    let row = Record::new(None, vec![]);
    let f0 = Box::new("abc".to_string());
    let f1 = Box::new("abc".to_string());
    let eq = Eq::new(f0, f1);
    assert!(matches!(eq.evaluate(&row), Field::Boolean(true)));
}

// #[test]
// fn test_ts_ts_eq() {
//     let f0 = Box::new(Timestamp::new(1));
//     let f1 = Box::new(Timestamp::new(1));
//     let eq = Eq::new(f0, f1);
//     assert!(matches!(eq.evaluate(), Field::Boolean(true)));
// }
