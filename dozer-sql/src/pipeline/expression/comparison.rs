use crate::pipeline::errors::PipelineError;
use dozer_types::rust_decimal::Decimal;
use dozer_types::types::Schema;
use dozer_types::{
    ordered_float::OrderedFloat,
    types::{Field, Record},
};
use num_traits::cast::*;

use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};

macro_rules! define_comparison {
    ($id:ident, $op:expr, $function:expr) => {
        pub fn $id(
            schema: &Schema,
            left: &Expression,
            right: &Expression,
            record: &Record,
        ) -> Result<Field, PipelineError> {
            let left_p = left.evaluate(&record, schema)?;
            let right_p = right.evaluate(&record, schema)?;

            match left_p {
                Field::Null => match right_p {
                    // left: Null, right: Null
                    Field::Null => Ok(Field::Boolean(true)),
                    _ => Ok(Field::Boolean(false)),
                },
                Field::Boolean(left_v) => match right_p {
                    // left: Bool, right: Bool
                    Field::Boolean(right_v) => Ok(Field::Boolean($function(left_v, right_v))),
                    _ => Ok(Field::Boolean(false)),
                },
                Field::Int(left_v) => match right_p {
                    // left: Int, right: Int
                    Field::Int(right_v) => Ok(Field::Boolean($function(left_v, right_v))),
                    // left: Int, right: UInt
                    Field::UInt(right_v) => Ok(Field::Boolean($function(
                        left_v,
                        right_v.to_i64().ok_or(PipelineError::UnableToCast(
                            format!("{}", right_v),
                            "i64".to_string(),
                        ))?,
                    ))),
                    // left: Int, right: Float
                    Field::Float(right_v) => {
                        let left_v_f = OrderedFloat::<f64>::from_i64(left_v).unwrap();
                        Ok(Field::Boolean($function(left_v_f, right_v)))
                    }
                    // left: Int, right: Decimal
                    Field::Decimal(right_v) => {
                        let left_v_d =
                            Decimal::from_i64(left_v).ok_or(PipelineError::UnableToCast(
                                format!("{}", left_v),
                                "Decimal".to_string(),
                            ))?;
                        Ok(Field::Boolean($function(left_v_d, right_v)))
                    }
                    // left: Int, right: Null
                    Field::Null => Ok(Field::Boolean(false)),
                    _ => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::UInt(left_v) => match right_p {
                    // left: UInt, right: Int
                    Field::Int(right_v) => Ok(Field::Boolean($function(
                        left_v.to_i64().ok_or(PipelineError::UnableToCast(
                            format!("{}", left_v),
                            "i64".to_string(),
                        ))?,
                        right_v,
                    ))),
                    // left: UInt, right: UInt
                    Field::UInt(right_v) => Ok(Field::Boolean($function(left_v, right_v))),
                    // left: UInt, right: Float
                    Field::Float(right_v) => {
                        let left_v_f = OrderedFloat::<f64>::from_i64(left_v.to_i64().ok_or(
                            PipelineError::UnableToCast(format!("{}", left_v), "i64".to_string()),
                        )?)
                        .unwrap();
                        Ok(Field::Boolean($function(left_v_f, right_v)))
                    }
                    // left: UInt, right: Decimal
                    Field::Decimal(right_v) => {
                        let left_v_d = Decimal::from_i64(left_v.to_i64().ok_or(
                            PipelineError::UnableToCast(format!("{}", left_v), "i64".to_string()),
                        )?)
                        .ok_or(PipelineError::UnableToCast(
                            format!("{}", left_v),
                            "Decimal".to_string(),
                        ))?;
                        Ok(Field::Boolean($function(left_v_d, right_v)))
                    }
                    // left: UInt, right: Null
                    Field::Null => Ok(Field::Boolean(false)),
                    _ => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::Float(left_v) => match right_p {
                    // left: Float, right: Float
                    Field::Float(right_v) => Ok(Field::Boolean($function(left_v, right_v))),
                    // left: Float, right: Int
                    Field::UInt(right_v) => {
                        let right_v_f = OrderedFloat::<f64>::from_i64(right_v.to_i64().ok_or(
                            PipelineError::UnableToCast(
                                format!("{}", right_v),
                                "Decimal".to_string(),
                            ),
                        )?)
                        .unwrap();
                        Ok(Field::Boolean($function(left_v, right_v_f)))
                    }
                    // left: Float, right: Int
                    Field::Int(right_v) => {
                        let right_v_f = OrderedFloat::<f64>::from_i64(right_v).unwrap();
                        Ok(Field::Boolean($function(left_v, right_v_f)))
                    }
                    // left: Float, right: Decimal
                    Field::Decimal(right_v) => {
                        let left_v_d = Decimal::from_f64(left_v.to_f64().ok_or(
                            PipelineError::UnableToCast(format!("{}", left_v), "i64".to_string()),
                        )?)
                        .ok_or(PipelineError::UnableToCast(
                            format!("{}", left_v),
                            "Decimal".to_string(),
                        ))?;
                        Ok(Field::Boolean($function(left_v_d, right_v)))
                    }
                    // left: Float, right: Null
                    Field::Null => Ok(Field::Boolean(false)),
                    _ => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::Decimal(left_v) => match right_p {
                    // left: Decimal, right: Float
                    Field::Float(right_v) => {
                        let right_v_d = Decimal::from_f64(right_v.to_f64().ok_or(
                            PipelineError::UnableToCast(format!("{}", right_v), "f64".to_string()),
                        )?)
                        .ok_or(PipelineError::UnableToCast(
                            format!("{}", right_v),
                            "Decimal".to_string(),
                        ))?;
                        Ok(Field::Boolean($function(left_v, right_v_d)))
                    }
                    // left: Decimal, right: Int
                    Field::Int(right_v) => {
                        let right_v_d =
                            Decimal::from_i64(right_v).ok_or(PipelineError::UnableToCast(
                                format!("{}", right_v),
                                "Decimal".to_string(),
                            ))?;
                        Ok(Field::Boolean($function(left_v, right_v_d)))
                    }
                    // left: Decimal, right: UInt
                    Field::UInt(right_v) => {
                        let right_v_d = Decimal::from_i64(right_v.to_i64().ok_or(
                            PipelineError::UnableToCast(format!("{}", right_v), "i64".to_string()),
                        )?)
                        .ok_or(PipelineError::UnableToCast(
                            format!("{}", right_v),
                            "Decimal".to_string(),
                        ))?;
                        Ok(Field::Boolean($function(left_v, right_v_d)))
                    }
                    // left: Decimal, right: Decimal
                    Field::Decimal(right_v) => Ok(Field::Boolean($function(left_v, right_v))),
                    // left: Decimal, right: Null
                    Field::Null => Ok(Field::Boolean(false)),
                    _ => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::String(ref left_v) => match right_p {
                    Field::String(ref right_v) => Ok(Field::Boolean($function(left_v, right_v))),
                    Field::Null => Ok(Field::Boolean(false)),
                    _ => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::Timestamp(left_v) => match right_p {
                    Field::Timestamp(right_v) => Ok(Field::Boolean($function(left_v, right_v))),
                    Field::Null => Ok(Field::Boolean(false)),
                    _ => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::Binary(ref _left_v) => Err(PipelineError::InvalidTypeComparison(
                    left_p,
                    right_p,
                    $op.to_string(),
                )),
                _ => Err(PipelineError::InvalidTypeComparison(
                    left_p,
                    right_p,
                    $op.to_string(),
                )),
            }
        }
    };
}

pub fn evaluate_lt(
    schema: &Schema,
    left: &Expression,
    right: &Expression,
    record: &Record,
) -> Result<Field, PipelineError> {
    let left_p = left.evaluate(record, schema)?;
    let right_p = right.evaluate(record, schema)?;

    match left_p {
        Field::Null => match right_p {
            Field::Null => Ok(Field::Boolean(true)),
            _ => Ok(Field::Boolean(false)),
        },
        Field::Boolean(left_v) => match right_p {
            Field::Boolean(right_v) => Ok(Field::Boolean(!left_v & right_v)),
            _ => Ok(Field::Boolean(false)),
        },
        Field::Int(left_v) => match right_p {
            Field::Int(right_v) => Ok(Field::Boolean(left_v < right_v)),
            Field::Float(right_v) => {
                let left_v_f = OrderedFloat::<f64>::from_i64(left_v).unwrap();
                Ok(Field::Boolean(left_v_f < right_v))
            }
            Field::Null => Ok(Field::Boolean(false)),
            _ => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                "<".to_string(),
            )),
        },
        Field::Float(left_v) => match right_p {
            Field::Float(right_v) => Ok(Field::Boolean(left_v < right_v)),
            Field::Int(right_v) => {
                let right_v_f = OrderedFloat::<f64>::from_i64(right_v).unwrap();
                Ok(Field::Boolean(left_v < right_v_f))
            }
            Field::Null => Ok(Field::Boolean(false)),
            _ => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                "<".to_string(),
            )),
        },
        Field::String(ref left_v) => match right_p {
            Field::String(ref right_v) => Ok(Field::Boolean(left_v < right_v)),
            Field::Null => Ok(Field::Boolean(false)),
            _ => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                "<".to_string(),
            )),
        },
        Field::Timestamp(left_v) => match right_p {
            Field::Timestamp(right_v) => Ok(Field::Boolean(left_v < right_v)),
            Field::Null => Ok(Field::Boolean(false)),
            _ => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                "<".to_string(),
            )),
        },
        Field::Binary(ref _left_v) => Err(PipelineError::InvalidTypeComparison(
            left_p,
            right_p,
            "<".to_string(),
        )),
        _ => Err(PipelineError::InvalidTypeComparison(
            left_p,
            right_p,
            "<".to_string(),
        )),
    }
}

pub fn evaluate_gt(
    schema: &Schema,
    left: &Expression,
    right: &Expression,
    record: &Record,
) -> Result<Field, PipelineError> {
    let left_p = left.evaluate(record, schema)?;
    let right_p = right.evaluate(record, schema)?;

    match left_p {
        Field::Null => match right_p {
            Field::Null => Ok(Field::Boolean(true)),
            _ => Ok(Field::Boolean(false)),
        },
        Field::Boolean(left_v) => match right_p {
            Field::Boolean(right_v) => Ok(Field::Boolean(left_v & !right_v)),
            _ => Ok(Field::Boolean(false)),
        },
        Field::Int(left_v) => match right_p {
            Field::Int(right_v) => Ok(Field::Boolean(left_v > right_v)),
            Field::Float(right_v) => {
                let left_v_f = OrderedFloat::<f64>::from_i64(left_v).unwrap();
                Ok(Field::Boolean(left_v_f > right_v))
            }
            Field::Null => Ok(Field::Boolean(false)),
            _ => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                ">".to_string(),
            )),
        },
        Field::Float(left_v) => match right_p {
            Field::Float(right_v) => Ok(Field::Boolean(left_v > right_v)),
            Field::Int(right_v) => {
                let right_v_f = OrderedFloat::<f64>::from_i64(right_v).unwrap();
                Ok(Field::Boolean(left_v > right_v_f))
            }
            Field::Null => Ok(Field::Boolean(false)),
            _ => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                ">".to_string(),
            )),
        },
        Field::String(ref left_v) => match right_p {
            Field::String(ref right_v) => Ok(Field::Boolean(left_v > right_v)),
            Field::Null => Ok(Field::Boolean(false)),
            _ => Err(PipelineError::InvalidTypeComparison(
                left_p.clone(),
                right_p,
                ">".to_string(),
            )),
        },
        Field::Timestamp(left_v) => match right_p {
            Field::Timestamp(right_v) => Ok(Field::Boolean(left_v > right_v)),
            Field::Null => Ok(Field::Boolean(false)),
            _ => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                ">".to_string(),
            )),
        },
        Field::Binary(ref _left_v) => Err(PipelineError::InvalidTypeComparison(
            left_p.clone(),
            right_p,
            ">".to_string(),
        )),
        _ => Err(PipelineError::InvalidTypeComparison(
            left_p,
            right_p,
            ">".to_string(),
        )),
    }
}

define_comparison!(evaluate_eq, "=", |l, r| { l == r });
define_comparison!(evaluate_ne, "!=", |l, r| { l != r });
define_comparison!(evaluate_lte, "<=", |l, r| { l <= r });
define_comparison!(evaluate_gte, ">=", |l, r| { l >= r });

#[cfg(test)]
use crate::pipeline::expression::execution::Expression::Literal;

#[test]
fn test_float_float_eq() {
    let row = Record::new(None, vec![], None);
    let f0 = Box::new(Literal(Field::Float(OrderedFloat(1.3))));
    let f1 = Box::new(Literal(Field::Float(OrderedFloat(1.3))));
    assert!(matches!(
        evaluate_eq(&Schema::empty(), &f0, &f1, &row),
        Ok(Field::Boolean(true))
    ));
}

#[test]
fn test_float_null_eq() {
    let row = Record::new(None, vec![], None);
    let f0 = Box::new(Literal(Field::Float(OrderedFloat(1.3))));
    let f1 = Box::new(Literal(Field::Null));
    assert!(matches!(
        evaluate_eq(&Schema::empty(), &f0, &f1, &row),
        Ok(Field::Boolean(false))
    ));
}

#[test]
fn test_float_int_eq() {
    let row = Record::new(None, vec![], None);
    let f0 = Box::new(Literal(Field::Float(OrderedFloat(1.0))));
    let f1 = Box::new(Literal(Field::Int(1)));
    assert!(matches!(
        evaluate_eq(&Schema::empty(), &f0, &f1, &row),
        Ok(Field::Boolean(true))
    ));
}

#[test]
fn test_decimal_decimal_eq() {
    let row = Record::new(None, vec![], None);
    let d0 = Box::new(Literal(Field::Decimal(Decimal::from_f64(1.3).unwrap())));
    let d1 = Box::new(Literal(Field::Decimal(Decimal::from_f64(1.3).unwrap())));
    assert!(matches!(
        evaluate_eq(&Schema::empty(), &d0, &d1, &row),
        Ok(Field::Boolean(true))
    ));
}

#[test]
fn test_decimal_null_eq() {
    let row = Record::new(None, vec![], None);
    let d0 = Box::new(Literal(Field::Decimal(Decimal::from_f64(1.3).unwrap())));
    let d1 = Box::new(Literal(Field::Null));
    assert!(matches!(
        evaluate_eq(&Schema::empty(), &d0, &d1, &row),
        Ok(Field::Boolean(false))
    ));
}

#[test]
fn test_decimal_int_eq() {
    let row = Record::new(None, vec![], None);
    let d0 = Box::new(Literal(Field::Decimal(Decimal::from_f64(1.0).unwrap())));
    let d1 = Box::new(Literal(Field::Int(1)));
    assert!(matches!(
        evaluate_eq(&Schema::empty(), &d0, &d1, &row),
        Ok(Field::Boolean(true))
    ));
}

#[test]
fn test_int_null_eq() {
    let row = Record::new(None, vec![], None);
    let f0 = Box::new(Literal(Field::Float(OrderedFloat(1.0))));
    let f1 = Box::new(Literal(Field::Null));
    assert!(matches!(
        evaluate_eq(&Schema::empty(), &f0, &f1, &row),
        Ok(Field::Boolean(false))
    ));
}

#[test]
fn test_int_float_eq() {
    let row = Record::new(None, vec![], None);
    let f0 = Box::new(Literal(Field::Int(1)));
    let f1 = Box::new(Literal(Field::Float(OrderedFloat(1.0))));
    assert!(matches!(
        evaluate_eq(&Schema::empty(), &f0, &f1, &row),
        Ok(Field::Boolean(true))
    ));
}

#[test]
fn test_uint_uint_eq() {
    let row = Record::new(None, vec![], None);
    let u0 = Box::new(Literal(Field::UInt(2)));
    let u1 = Box::new(Literal(Field::UInt(2)));
    assert!(matches!(
        evaluate_eq(&Schema::empty(), &u0, &u1, &row),
        Ok(Field::Boolean(true))
    ));
}

#[test]
fn test_uint_null_eq() {
    let row = Record::new(None, vec![], None);
    let u0 = Box::new(Literal(Field::UInt(2)));
    let u1 = Box::new(Literal(Field::Null));
    assert!(matches!(
        evaluate_eq(&Schema::empty(), &u0, &u1, &row),
        Ok(Field::Boolean(false))
    ));
}

#[test]
fn test_uint_decimal_eq() {
    let row = Record::new(None, vec![], None);
    let u0 = Box::new(Literal(Field::Decimal(Decimal::from_f64(2.0).unwrap())));
    let u1 = Box::new(Literal(Field::UInt(2)));
    assert!(matches!(
        evaluate_eq(&Schema::empty(), &u0, &u1, &row),
        Ok(Field::Boolean(true))
    ));
}

#[test]
fn test_bool_bool_eq() {
    let row = Record::new(None, vec![], None);
    let f0 = Box::new(Literal(Field::Boolean(false)));
    let f1 = Box::new(Literal(Field::Boolean(false)));
    assert!(matches!(
        evaluate_eq(&Schema::empty(), &f0, &f1, &row),
        Ok(Field::Boolean(true))
    ));
}

#[test]
fn test_str_str_eq() {
    let row = Record::new(None, vec![], None);
    let f0 = Box::new(Literal(Field::String("abc".to_string())));
    let f1 = Box::new(Literal(Field::String("abc".to_string())));
    assert!(matches!(
        evaluate_eq(&Schema::empty(), &f0, &f1, &row),
        Ok(Field::Boolean(true))
    ));
}

#[test]
fn test_str_null_eq() {
    let row = Record::new(None, vec![], None);
    let f0 = Box::new(Literal(Field::String("abc".to_string())));
    let f1 = Box::new(Literal(Field::Null));
    assert!(matches!(
        evaluate_eq(&Schema::empty(), &f0, &f1, &row),
        Ok(Field::Boolean(false))
    ));
}
