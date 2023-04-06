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
                Field::Null => Ok(Field::Null),
                Field::Boolean(left_v) => match right_p {
                    // left: Bool, right: Bool
                    Field::Boolean(right_v) => Ok(Field::Boolean($function(left_v, right_v))),
                    Field::UInt(_)
                    | Field::U128(_)
                    | Field::Int(_)
                    | Field::I128(_)
                    | Field::Float(_)
                    | Field::String(_)
                    | Field::Text(_)
                    | Field::Binary(_)
                    | Field::Decimal(_)
                    | Field::Timestamp(_)
                    | Field::Date(_)
                    | Field::Bson(_)
                    | Field::Point(_)
                    | Field::Duration(_)
                    | Field::Null => Ok(Field::Null),
                },
                Field::Int(left_v) => match right_p {
                    // left: Int, right: Int
                    Field::Int(right_v) => Ok(Field::Boolean($function(left_v, right_v))),
                    // left: Int, right: I128
                    Field::I128(right_v) => Ok(Field::Boolean($function(left_v as i128, right_v))),
                    // left: Int, right: UInt
                    Field::UInt(right_v) => Ok(Field::Boolean($function(left_v, right_v as i64))),
                    // left: Int, right: U128
                    Field::U128(right_v) => Ok(Field::Boolean($function(left_v, right_v as i64))),
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
                    Field::Null => Ok(Field::Null),
                    Field::Boolean(_)
                    | Field::String(_)
                    | Field::Text(_)
                    | Field::Binary(_)
                    | Field::Timestamp(_)
                    | Field::Date(_)
                    | Field::Bson(_)
                    | Field::Point(_)
                    | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::I128(left_v) => match right_p {
                    // left: I128, right: Int
                    Field::Int(right_v) => Ok(Field::Boolean($function(left_v, right_v as i128))),
                    // left: I128, right: I128
                    Field::I128(right_v) => Ok(Field::Boolean($function(left_v, right_v))),
                    // left: I128, right: UInt
                    Field::UInt(right_v) => Ok(Field::Boolean($function(left_v, right_v as i128))),
                    // left: I128, right: U128
                    Field::U128(right_v) => Ok(Field::Boolean($function(left_v, right_v as i128))),
                    // left: I128, right: Float
                    Field::Float(right_v) => {
                        let left_v_f = OrderedFloat::<f64>::from_i128(left_v).unwrap();
                        Ok(Field::Boolean($function(left_v_f, right_v)))
                    }
                    // left: I128, right: Decimal
                    Field::Decimal(right_v) => {
                        let left_v_d =
                            Decimal::from_i128(left_v).ok_or(PipelineError::UnableToCast(
                                format!("{}", left_v),
                                "Decimal".to_string(),
                            ))?;
                        Ok(Field::Boolean($function(left_v_d, right_v)))
                    }
                    // left: I128, right: Null
                    Field::Null => Ok(Field::Null),
                    Field::Boolean(_)
                    | Field::String(_)
                    | Field::Text(_)
                    | Field::Binary(_)
                    | Field::Timestamp(_)
                    | Field::Date(_)
                    | Field::Bson(_)
                    | Field::Point(_)
                    | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::UInt(left_v) => match right_p {
                    // left: UInt, right: Int
                    Field::Int(right_v) => Ok(Field::Boolean($function(left_v as i64, right_v))),
                    // left: UInt, right: I128
                    Field::I128(right_v) => Ok(Field::Boolean($function(left_v as i128, right_v))),
                    // left: UInt, right: UInt
                    Field::UInt(right_v) => Ok(Field::Boolean($function(left_v, right_v))),
                    // left: UInt, right: U128
                    Field::U128(right_v) => Ok(Field::Boolean($function(left_v as u128, right_v))),
                    // left: UInt, right: Float
                    Field::Float(right_v) => {
                        let left_v_f = OrderedFloat(left_v as f64);
                        Ok(Field::Boolean($function(left_v_f, right_v)))
                    }
                    // left: UInt, right: Decimal
                    Field::Decimal(right_v) => {
                        let left_v_d =
                            Decimal::from_f64(left_v as f64).ok_or(PipelineError::UnableToCast(
                                format!("{}", left_v),
                                "Decimal".to_string(),
                            ))?;
                        Ok(Field::Boolean($function(left_v_d, right_v)))
                    }
                    // left: UInt, right: Null
                    Field::Null => Ok(Field::Null),
                    Field::Boolean(_)
                    | Field::String(_)
                    | Field::Text(_)
                    | Field::Binary(_)
                    | Field::Timestamp(_)
                    | Field::Date(_)
                    | Field::Bson(_)
                    | Field::Point(_)
                    | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::U128(left_v) => match right_p {
                    // left: U128, right: Int
                    Field::Int(right_v) => {
                        Ok(Field::Boolean($function(left_v as i128, right_v as i128)))
                    }
                    // left: U128, right: I128
                    Field::I128(right_v) => Ok(Field::Boolean($function(left_v as i128, right_v))),
                    // left: U128, right: UInt
                    Field::UInt(right_v) => Ok(Field::Boolean($function(left_v, right_v as u128))),
                    // left: U128, right: U128
                    Field::U128(right_v) => Ok(Field::Boolean($function(left_v, right_v))),
                    // left: U128, right: Float
                    Field::Float(right_v) => {
                        let left_v_f = OrderedFloat(left_v as f64);
                        Ok(Field::Boolean($function(left_v_f, right_v)))
                    }
                    // left: U128, right: Decimal
                    Field::Decimal(right_v) => {
                        let left_v_d =
                            Decimal::from_f64(left_v as f64).ok_or(PipelineError::UnableToCast(
                                format!("{}", left_v),
                                "Decimal".to_string(),
                            ))?;
                        Ok(Field::Boolean($function(left_v_d, right_v)))
                    }
                    // left: U128, right: Null
                    Field::Null => Ok(Field::Null),
                    Field::Boolean(_)
                    | Field::String(_)
                    | Field::Text(_)
                    | Field::Binary(_)
                    | Field::Timestamp(_)
                    | Field::Date(_)
                    | Field::Bson(_)
                    | Field::Point(_)
                    | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::Float(left_v) => match right_p {
                    // left: Float, right: Float
                    Field::Float(right_v) => Ok(Field::Boolean($function(left_v, right_v))),
                    // left: Float, right: UInt
                    Field::UInt(right_v) => {
                        let right_v_f = OrderedFloat(right_v as f64);
                        Ok(Field::Boolean($function(left_v, right_v_f)))
                    }
                    // left: Float, right: U128
                    Field::U128(right_v) => {
                        let right_v_f = OrderedFloat(right_v as f64);
                        Ok(Field::Boolean($function(left_v, right_v_f)))
                    }
                    // left: Float, right: Int
                    Field::Int(right_v) => {
                        let right_v_f = OrderedFloat(right_v as f64);
                        Ok(Field::Boolean($function(left_v, right_v_f)))
                    }
                    // left: Float, right: I128
                    Field::I128(right_v) => {
                        let right_v_f = OrderedFloat(right_v as f64);
                        Ok(Field::Boolean($function(left_v, right_v_f)))
                    }
                    // left: Float, right: Decimal
                    Field::Decimal(right_v) => {
                        let left_v_d =
                            Decimal::from_f64(*left_v).ok_or(PipelineError::UnableToCast(
                                format!("{}", left_v),
                                "Decimal".to_string(),
                            ))?;
                        Ok(Field::Boolean($function(left_v_d, right_v)))
                    }
                    // left: Float, right: Null
                    Field::Null => Ok(Field::Null),
                    Field::Boolean(_)
                    | Field::String(_)
                    | Field::Text(_)
                    | Field::Binary(_)
                    | Field::Timestamp(_)
                    | Field::Date(_)
                    | Field::Bson(_)
                    | Field::Point(_)
                    | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::Decimal(left_v) => match right_p {
                    // left: Decimal, right: Float
                    Field::Float(right_v) => {
                        let right_v_d =
                            Decimal::from_f64(*right_v).ok_or(PipelineError::UnableToCast(
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
                    // left: Decimal, right: I128
                    Field::I128(right_v) => {
                        let right_v_d =
                            Decimal::from_i128(right_v).ok_or(PipelineError::UnableToCast(
                                format!("{}", right_v),
                                "Decimal".to_string(),
                            ))?;
                        Ok(Field::Boolean($function(left_v, right_v_d)))
                    }
                    // left: Decimal, right: UInt
                    Field::UInt(right_v) => {
                        let right_v_d =
                            Decimal::from_u64(right_v).ok_or(PipelineError::UnableToCast(
                                format!("{}", right_v),
                                "Decimal".to_string(),
                            ))?;
                        Ok(Field::Boolean($function(left_v, right_v_d)))
                    }
                    // left: Decimal, right: U128
                    Field::U128(right_v) => {
                        let right_v_d =
                            Decimal::from_u128(right_v).ok_or(PipelineError::UnableToCast(
                                format!("{}", right_v),
                                "Decimal".to_string(),
                            ))?;
                        Ok(Field::Boolean($function(left_v, right_v_d)))
                    }
                    // left: Decimal, right: Decimal
                    Field::Decimal(right_v) => Ok(Field::Boolean($function(left_v, right_v))),
                    // left: Decimal, right: Null
                    Field::Null => Ok(Field::Null),
                    Field::Boolean(_)
                    | Field::String(_)
                    | Field::Text(_)
                    | Field::Binary(_)
                    | Field::Timestamp(_)
                    | Field::Date(_)
                    | Field::Bson(_)
                    | Field::Point(_)
                    | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::String(ref left_v) => match right_p {
                    Field::String(ref right_v) => Ok(Field::Boolean($function(left_v, right_v))),
                    Field::Text(ref right_v) => Ok(Field::Boolean($function(left_v, right_v))),
                    Field::Null => Ok(Field::Null),
                    Field::UInt(_)
                    | Field::U128(_)
                    | Field::Int(_)
                    | Field::I128(_)
                    | Field::Float(_)
                    | Field::Boolean(_)
                    | Field::Binary(_)
                    | Field::Decimal(_)
                    | Field::Timestamp(_)
                    | Field::Date(_)
                    | Field::Bson(_)
                    | Field::Point(_)
                    | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::Text(ref left_v) => match right_p {
                    Field::String(ref right_v) => Ok(Field::Boolean($function(left_v, right_v))),
                    Field::Text(ref right_v) => Ok(Field::Boolean($function(left_v, right_v))),
                    Field::Null => Ok(Field::Null),
                    Field::UInt(_)
                    | Field::U128(_)
                    | Field::Int(_)
                    | Field::I128(_)
                    | Field::Float(_)
                    | Field::Boolean(_)
                    | Field::Binary(_)
                    | Field::Decimal(_)
                    | Field::Timestamp(_)
                    | Field::Date(_)
                    | Field::Bson(_)
                    | Field::Point(_)
                    | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::Timestamp(left_v) => match right_p {
                    Field::Timestamp(right_v) => Ok(Field::Boolean($function(left_v, right_v))),
                    Field::Null => Ok(Field::Null),
                    Field::UInt(_)
                    | Field::U128(_)
                    | Field::Int(_)
                    | Field::I128(_)
                    | Field::Float(_)
                    | Field::Boolean(_)
                    | Field::String(_)
                    | Field::Text(_)
                    | Field::Binary(_)
                    | Field::Decimal(_)
                    | Field::Date(_)
                    | Field::Bson(_)
                    | Field::Point(_)
                    | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::Date(left_v) => match right_p {
                    Field::Date(right_v) => Ok(Field::Boolean($function(left_v, right_v))),
                    Field::Null => Ok(Field::Null),
                    Field::UInt(_)
                    | Field::U128(_)
                    | Field::Int(_)
                    | Field::I128(_)
                    | Field::Float(_)
                    | Field::Boolean(_)
                    | Field::String(_)
                    | Field::Text(_)
                    | Field::Binary(_)
                    | Field::Decimal(_)
                    | Field::Timestamp(_)
                    | Field::Bson(_)
                    | Field::Point(_)
                    | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::Binary(_) | Field::Bson(_) | Field::Point(_) | Field::Duration(_) => Err(
                    PipelineError::InvalidTypeComparison(left_p, right_p, $op.to_string()),
                ),
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
        Field::Null => Ok(Field::Null),
        Field::Boolean(left_v) => match right_p {
            // left: Bool, right: Bool
            Field::Boolean(right_v) => Ok(Field::Boolean(!left_v & right_v)),
            Field::UInt(_)
            | Field::U128(_)
            | Field::Int(_)
            | Field::I128(_)
            | Field::Float(_)
            | Field::String(_)
            | Field::Text(_)
            | Field::Binary(_)
            | Field::Decimal(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_)
            | Field::Duration(_)
            | Field::Null => Ok(Field::Null),
        },
        Field::Int(left_v) => match right_p {
            // left: Int, right: Int
            Field::Int(right_v) => Ok(Field::Boolean(left_v < right_v)),
            // left: Int, right: I128
            Field::I128(right_v) => Ok(Field::Boolean((left_v as i128) < right_v)),
            // left: Int, right: UInt
            Field::UInt(right_v) => Ok(Field::Boolean(left_v < (right_v as i64))),
            // left: Int, right: U128
            Field::U128(right_v) => Ok(Field::Boolean(left_v < (right_v as i64))),
            // left: Int, right: Float
            Field::Float(right_v) => {
                let left_v_f = OrderedFloat::<f64>::from_i64(left_v).unwrap();
                Ok(Field::Boolean(left_v_f < right_v))
            }
            // left: Int, right: Decimal
            Field::Decimal(right_v) => {
                let left_v_d = Decimal::from_i64(left_v).ok_or(PipelineError::UnableToCast(
                    format!("{}", left_v),
                    "Decimal".to_string(),
                ))?;
                Ok(Field::Boolean(left_v_d < right_v))
            }
            // left: Int, right: Null
            Field::Null => Ok(Field::Null),
            Field::Boolean(_)
            | Field::String(_)
            | Field::Text(_)
            | Field::Binary(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                "<".to_string(),
            )),
        },
        Field::I128(left_v) => match right_p {
            // left: I128, right: Int
            Field::Int(right_v) => Ok(Field::Boolean(left_v < (right_v as i128))),
            // left: I128, right: I128
            Field::I128(right_v) => Ok(Field::Boolean(left_v < right_v)),
            // left: I128, right: UInt
            Field::UInt(right_v) => Ok(Field::Boolean(left_v < (right_v as i128))),
            // left: I128, right: U128
            Field::U128(right_v) => Ok(Field::Boolean(left_v < (right_v as i128))),
            // left: I128, right: Float
            Field::Float(right_v) => {
                let left_v_f = OrderedFloat::<f64>::from_i128(left_v).unwrap();
                Ok(Field::Boolean(left_v_f < right_v))
            }
            // left: I128, right: Decimal
            Field::Decimal(right_v) => {
                let left_v_d = Decimal::from_i128(left_v).ok_or(PipelineError::UnableToCast(
                    format!("{}", left_v),
                    "Decimal".to_string(),
                ))?;
                Ok(Field::Boolean(left_v_d < right_v))
            }
            // left: I128, right: Null
            Field::Null => Ok(Field::Null),
            Field::Boolean(_)
            | Field::String(_)
            | Field::Text(_)
            | Field::Binary(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                "<".to_string(),
            )),
        },
        Field::UInt(left_v) => match right_p {
            // left: UInt, right: Int
            Field::Int(right_v) => Ok(Field::Boolean((left_v as i64) < right_v)),
            // left: UInt, right: I128
            Field::I128(right_v) => Ok(Field::Boolean((left_v as i128) < right_v)),
            // left: UInt, right: UInt
            Field::UInt(right_v) => Ok(Field::Boolean(left_v < right_v)),
            // left: UInt, right: U128
            Field::U128(right_v) => Ok(Field::Boolean((left_v as u128) < right_v)),
            // left: UInt, right: Float
            Field::Float(right_v) => {
                let left_v_f = OrderedFloat(left_v as f64);
                Ok(Field::Boolean(left_v_f < right_v))
            }
            // left: UInt, right: Decimal
            Field::Decimal(right_v) => {
                let left_v_d = Decimal::from_f64(left_v as f64).ok_or(
                    PipelineError::UnableToCast(format!("{}", left_v), "Decimal".to_string()),
                )?;
                Ok(Field::Boolean(left_v_d < right_v))
            }
            // left: UInt, right: Null
            Field::Null => Ok(Field::Null),
            Field::Boolean(_)
            | Field::String(_)
            | Field::Text(_)
            | Field::Binary(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                "<".to_string(),
            )),
        },
        Field::U128(left_v) => match right_p {
            // left: U128, right: Int
            Field::Int(right_v) => Ok(Field::Boolean((left_v as i128) < (right_v as i128))),
            // left: U128, right: I128
            Field::I128(right_v) => Ok(Field::Boolean((left_v as i128) < right_v)),
            // left: U128, right: UInt
            Field::UInt(right_v) => Ok(Field::Boolean(left_v < (right_v as u128))),
            // left: U128, right: U128
            Field::U128(right_v) => Ok(Field::Boolean(left_v < right_v)),
            // left: U128, right: Float
            Field::Float(right_v) => {
                let left_v_f = OrderedFloat(left_v as f64);
                Ok(Field::Boolean(left_v_f < right_v))
            }
            // left: U128, right: Decimal
            Field::Decimal(right_v) => {
                let left_v_d = Decimal::from_f64(left_v as f64).ok_or(
                    PipelineError::UnableToCast(format!("{}", left_v), "Decimal".to_string()),
                )?;
                Ok(Field::Boolean(left_v_d < right_v))
            }
            // left: U128, right: Null
            Field::Null => Ok(Field::Null),
            Field::Boolean(_)
            | Field::String(_)
            | Field::Text(_)
            | Field::Binary(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                "<".to_string(),
            )),
        },
        Field::Float(left_v) => match right_p {
            // left: Float, right: Float
            Field::Float(right_v) => Ok(Field::Boolean(left_v < right_v)),
            // left: Float, right: UInt
            Field::UInt(right_v) => {
                let right_v_f = OrderedFloat(right_v as f64);
                Ok(Field::Boolean(left_v < right_v_f))
            }
            // left: Float, right: U128
            Field::U128(right_v) => {
                let right_v_f = OrderedFloat(right_v as f64);
                Ok(Field::Boolean(left_v < right_v_f))
            }
            // left: Float, right: Int
            Field::Int(right_v) => {
                let right_v_f = OrderedFloat(right_v as f64);
                Ok(Field::Boolean(left_v < right_v_f))
            }
            // left: Float, right: I128
            Field::I128(right_v) => {
                let right_v_f = OrderedFloat(right_v as f64);
                Ok(Field::Boolean(left_v < right_v_f))
            }
            // left: Float, right: Decimal
            Field::Decimal(right_v) => {
                let left_v_d = Decimal::from_f64(*left_v).ok_or(PipelineError::UnableToCast(
                    format!("{}", left_v),
                    "Decimal".to_string(),
                ))?;
                Ok(Field::Boolean(left_v_d < right_v))
            }
            // left: Float, right: Null
            Field::Null => Ok(Field::Null),
            Field::Boolean(_)
            | Field::String(_)
            | Field::Text(_)
            | Field::Binary(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                "<".to_string(),
            )),
        },
        Field::Decimal(left_v) => {
            match right_p {
                // left: Decimal, right: Float
                Field::Float(right_v) => {
                    let right_v_d = Decimal::from_f64(*right_v).ok_or(
                        PipelineError::UnableToCast(format!("{}", right_v), "Decimal".to_string()),
                    )?;
                    Ok(Field::Boolean(left_v < right_v_d))
                }
                // left: Decimal, right: Int
                Field::Int(right_v) => {
                    let right_v_d = Decimal::from_i64(right_v).ok_or(
                        PipelineError::UnableToCast(format!("{}", right_v), "Decimal".to_string()),
                    )?;
                    Ok(Field::Boolean(left_v < right_v_d))
                }
                // left: Decimal, right: I128
                Field::I128(right_v) => {
                    let right_v_d = Decimal::from_i128(right_v).ok_or(
                        PipelineError::UnableToCast(format!("{}", right_v), "Decimal".to_string()),
                    )?;
                    Ok(Field::Boolean(left_v < right_v_d))
                }
                // left: Decimal, right: UInt
                Field::UInt(right_v) => {
                    let right_v_d = Decimal::from_u64(right_v).ok_or(
                        PipelineError::UnableToCast(format!("{}", right_v), "Decimal".to_string()),
                    )?;
                    Ok(Field::Boolean(left_v < right_v_d))
                }
                // left: Decimal, right: U128
                Field::U128(right_v) => {
                    let right_v_d = Decimal::from_u128(right_v).ok_or(
                        PipelineError::UnableToCast(format!("{}", right_v), "Decimal".to_string()),
                    )?;
                    Ok(Field::Boolean(left_v < right_v_d))
                }
                // left: Decimal, right: Decimal
                Field::Decimal(right_v) => Ok(Field::Boolean(left_v < right_v)),
                // left: Decimal, right: Null
                Field::Null => Ok(Field::Null),
                Field::Boolean(_)
                | Field::String(_)
                | Field::Text(_)
                | Field::Binary(_)
                | Field::Timestamp(_)
                | Field::Date(_)
                | Field::Bson(_)
                | Field::Point(_)
                | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                    left_p,
                    right_p,
                    "<".to_string(),
                )),
            }
        }
        Field::String(ref left_v) => match right_p {
            Field::String(ref right_v) => Ok(Field::Boolean(left_v < right_v)),
            Field::Text(ref right_v) => Ok(Field::Boolean(left_v < right_v)),
            Field::Null => Ok(Field::Null),
            Field::UInt(_)
            | Field::U128(_)
            | Field::Int(_)
            | Field::I128(_)
            | Field::Float(_)
            | Field::Boolean(_)
            | Field::Binary(_)
            | Field::Decimal(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                "<".to_string(),
            )),
        },
        Field::Text(ref left_v) => match right_p {
            Field::String(ref right_v) => Ok(Field::Boolean(left_v < right_v)),
            Field::Text(ref right_v) => Ok(Field::Boolean(left_v < right_v)),
            Field::Null => Ok(Field::Null),
            Field::UInt(_)
            | Field::U128(_)
            | Field::Int(_)
            | Field::I128(_)
            | Field::Float(_)
            | Field::Boolean(_)
            | Field::Binary(_)
            | Field::Decimal(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                "<".to_string(),
            )),
        },
        Field::Timestamp(left_v) => match right_p {
            Field::Timestamp(right_v) => Ok(Field::Boolean(left_v < right_v)),
            Field::Null => Ok(Field::Null),
            Field::UInt(_)
            | Field::U128(_)
            | Field::Int(_)
            | Field::I128(_)
            | Field::Float(_)
            | Field::Boolean(_)
            | Field::String(_)
            | Field::Text(_)
            | Field::Binary(_)
            | Field::Decimal(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                "<".to_string(),
            )),
        },
        Field::Date(left_v) => match right_p {
            Field::Date(right_v) => Ok(Field::Boolean(left_v < right_v)),
            Field::Null => Ok(Field::Null),
            Field::UInt(_)
            | Field::U128(_)
            | Field::Int(_)
            | Field::I128(_)
            | Field::Float(_)
            | Field::Boolean(_)
            | Field::String(_)
            | Field::Text(_)
            | Field::Binary(_)
            | Field::Decimal(_)
            | Field::Timestamp(_)
            | Field::Bson(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                "<".to_string(),
            )),
        },
        Field::Binary(_) | Field::Bson(_) | Field::Point(_) | Field::Duration(_) => Err(
            PipelineError::InvalidTypeComparison(left_p, right_p, "<".to_string()),
        ),
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
        Field::Null => Ok(Field::Null),
        Field::Boolean(left_v) => match right_p {
            // left: Bool, right: Bool
            Field::Boolean(right_v) => Ok(Field::Boolean(left_v & !right_v)),
            Field::UInt(_)
            | Field::U128(_)
            | Field::Int(_)
            | Field::I128(_)
            | Field::Float(_)
            | Field::String(_)
            | Field::Text(_)
            | Field::Binary(_)
            | Field::Decimal(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_)
            | Field::Duration(_)
            | Field::Null => Ok(Field::Null),
        },
        Field::Int(left_v) => match right_p {
            // left: Int, right: Int
            Field::Int(right_v) => Ok(Field::Boolean(left_v > right_v)),
            // left: Int, right: I128
            Field::I128(right_v) => Ok(Field::Boolean((left_v as i128) > right_v)),
            // left: Int, right: UInt
            Field::UInt(right_v) => Ok(Field::Boolean(left_v > (right_v as i64))),
            // left: Int, right: U128
            Field::U128(right_v) => Ok(Field::Boolean(left_v > (right_v as i64))),
            // left: Int, right: Float
            Field::Float(right_v) => {
                let left_v_f = OrderedFloat::<f64>::from_i64(left_v).unwrap();
                Ok(Field::Boolean(left_v_f > right_v))
            }
            // left: Int, right: Decimal
            Field::Decimal(right_v) => {
                let left_v_d = Decimal::from_i64(left_v).ok_or(PipelineError::UnableToCast(
                    format!("{}", left_v),
                    "Decimal".to_string(),
                ))?;
                Ok(Field::Boolean(left_v_d > right_v))
            }
            // left: Int, right: Null
            Field::Null => Ok(Field::Null),
            Field::Boolean(_)
            | Field::String(_)
            | Field::Text(_)
            | Field::Binary(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                ">".to_string(),
            )),
        },
        Field::I128(left_v) => match right_p {
            // left: I128, right: Int
            Field::Int(right_v) => Ok(Field::Boolean(left_v > (right_v as i128))),
            // left: I128, right: I128
            Field::I128(right_v) => Ok(Field::Boolean(left_v > right_v)),
            // left: I128, right: UInt
            Field::UInt(right_v) => Ok(Field::Boolean(left_v > (right_v as i128))),
            // left: I128, right: U128
            Field::U128(right_v) => Ok(Field::Boolean(left_v > (right_v as i128))),
            // left: I128, right: Float
            Field::Float(right_v) => {
                let left_v_f = OrderedFloat::<f64>::from_i128(left_v).unwrap();
                Ok(Field::Boolean(left_v_f > right_v))
            }
            // left: I128, right: Decimal
            Field::Decimal(right_v) => {
                let left_v_d = Decimal::from_i128(left_v).ok_or(PipelineError::UnableToCast(
                    format!("{}", left_v),
                    "Decimal".to_string(),
                ))?;
                Ok(Field::Boolean(left_v_d > right_v))
            }
            // left: I128, right: Null
            Field::Null => Ok(Field::Null),
            Field::Boolean(_)
            | Field::String(_)
            | Field::Text(_)
            | Field::Binary(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                ">".to_string(),
            )),
        },
        Field::UInt(left_v) => match right_p {
            // left: UInt, right: Int
            Field::Int(right_v) => Ok(Field::Boolean((left_v as i64) > right_v)),
            // left: UInt, right: I128
            Field::I128(right_v) => Ok(Field::Boolean((left_v as i128) > right_v)),
            // left: UInt, right: UInt
            Field::UInt(right_v) => Ok(Field::Boolean(left_v > right_v)),
            // left: UInt, right: U128
            Field::U128(right_v) => Ok(Field::Boolean((left_v as u128) > right_v)),
            // left: UInt, right: Float
            Field::Float(right_v) => {
                let left_v_f = OrderedFloat(left_v as f64);
                Ok(Field::Boolean(left_v_f > right_v))
            }
            // left: UInt, right: Decimal
            Field::Decimal(right_v) => {
                let left_v_d = Decimal::from_f64(left_v as f64).ok_or(
                    PipelineError::UnableToCast(format!("{}", left_v), "Decimal".to_string()),
                )?;
                Ok(Field::Boolean(left_v_d > right_v))
            }
            // left: UInt, right: Null
            Field::Null => Ok(Field::Null),
            Field::Boolean(_)
            | Field::String(_)
            | Field::Text(_)
            | Field::Binary(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                ">".to_string(),
            )),
        },
        Field::U128(left_v) => match right_p {
            // left: U128, right: Int
            Field::Int(right_v) => Ok(Field::Boolean((left_v as i128) > (right_v as i128))),
            // left: U128, right: I128
            Field::I128(right_v) => Ok(Field::Boolean((left_v as i128) > right_v)),
            // left: U128, right: UInt
            Field::UInt(right_v) => Ok(Field::Boolean(left_v > (right_v as u128))),
            // left: U128, right: U128
            Field::U128(right_v) => Ok(Field::Boolean(left_v > right_v)),
            // left: U128, right: Float
            Field::Float(right_v) => {
                let left_v_f = OrderedFloat(left_v as f64);
                Ok(Field::Boolean(left_v_f > right_v))
            }
            // left: U128, right: Decimal
            Field::Decimal(right_v) => {
                let left_v_d = Decimal::from_f64(left_v as f64).ok_or(
                    PipelineError::UnableToCast(format!("{}", left_v), "Decimal".to_string()),
                )?;
                Ok(Field::Boolean(left_v_d > right_v))
            }
            // left: U128, right: Null
            Field::Null => Ok(Field::Null),
            Field::Boolean(_)
            | Field::String(_)
            | Field::Text(_)
            | Field::Binary(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                ">".to_string(),
            )),
        },
        Field::Float(left_v) => match right_p {
            // left: Float, right: Float
            Field::Float(right_v) => Ok(Field::Boolean(left_v > right_v)),
            // left: Float, right: UInt
            Field::UInt(right_v) => {
                let right_v_f = OrderedFloat(right_v as f64);
                Ok(Field::Boolean(left_v > right_v_f))
            }
            // left: Float, right: U128
            Field::U128(right_v) => {
                let right_v_f = OrderedFloat(right_v as f64);
                Ok(Field::Boolean(left_v > right_v_f))
            }
            // left: Float, right: Int
            Field::Int(right_v) => {
                let right_v_f = OrderedFloat(right_v as f64);
                Ok(Field::Boolean(left_v > right_v_f))
            }
            // left: Float, right: I128
            Field::I128(right_v) => {
                let right_v_f = OrderedFloat(right_v as f64);
                Ok(Field::Boolean(left_v > right_v_f))
            }
            // left: Float, right: Decimal
            Field::Decimal(right_v) => {
                let left_v_d = Decimal::from_f64(*left_v).ok_or(PipelineError::UnableToCast(
                    format!("{}", left_v),
                    "Decimal".to_string(),
                ))?;
                Ok(Field::Boolean(left_v_d > right_v))
            }
            // left: Float, right: Null
            Field::Null => Ok(Field::Null),
            Field::Boolean(_)
            | Field::String(_)
            | Field::Text(_)
            | Field::Binary(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                ">".to_string(),
            )),
        },
        Field::Decimal(left_v) => {
            match right_p {
                // left: Decimal, right: Float
                Field::Float(right_v) => {
                    let right_v_d = Decimal::from_f64(*right_v).ok_or(
                        PipelineError::UnableToCast(format!("{}", right_v), "Decimal".to_string()),
                    )?;
                    Ok(Field::Boolean(left_v > right_v_d))
                }
                // left: Decimal, right: Int
                Field::Int(right_v) => {
                    let right_v_d = Decimal::from_i64(right_v).ok_or(
                        PipelineError::UnableToCast(format!("{}", right_v), "Decimal".to_string()),
                    )?;
                    Ok(Field::Boolean(left_v > right_v_d))
                }
                // left: Decimal, right: I128
                Field::I128(right_v) => {
                    let right_v_d = Decimal::from_i128(right_v).ok_or(
                        PipelineError::UnableToCast(format!("{}", right_v), "Decimal".to_string()),
                    )?;
                    Ok(Field::Boolean(left_v > right_v_d))
                }
                // left: Decimal, right: UInt
                Field::UInt(right_v) => {
                    let right_v_d = Decimal::from_u64(right_v).ok_or(
                        PipelineError::UnableToCast(format!("{}", right_v), "Decimal".to_string()),
                    )?;
                    Ok(Field::Boolean(left_v > right_v_d))
                }
                // left: Decimal, right: U128
                Field::U128(right_v) => {
                    let right_v_d = Decimal::from_u128(right_v).ok_or(
                        PipelineError::UnableToCast(format!("{}", right_v), "Decimal".to_string()),
                    )?;
                    Ok(Field::Boolean(left_v > right_v_d))
                }
                // left: Decimal, right: Decimal
                Field::Decimal(right_v) => Ok(Field::Boolean(left_v > right_v)),
                // left: Decimal, right: Null
                Field::Null => Ok(Field::Null),
                Field::Boolean(_)
                | Field::String(_)
                | Field::Text(_)
                | Field::Binary(_)
                | Field::Timestamp(_)
                | Field::Date(_)
                | Field::Bson(_)
                | Field::Point(_)
                | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                    left_p,
                    right_p,
                    ">".to_string(),
                )),
            }
        }
        Field::String(ref left_v) => match right_p {
            Field::String(ref right_v) => Ok(Field::Boolean(left_v > right_v)),
            Field::Text(ref right_v) => Ok(Field::Boolean(left_v > right_v)),
            Field::Null => Ok(Field::Null),
            Field::UInt(_)
            | Field::U128(_)
            | Field::Int(_)
            | Field::I128(_)
            | Field::Float(_)
            | Field::Boolean(_)
            | Field::Binary(_)
            | Field::Decimal(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                ">".to_string(),
            )),
        },
        Field::Text(ref left_v) => match right_p {
            Field::String(ref right_v) => Ok(Field::Boolean(left_v > right_v)),
            Field::Text(ref right_v) => Ok(Field::Boolean(left_v > right_v)),
            Field::Null => Ok(Field::Null),
            Field::UInt(_)
            | Field::U128(_)
            | Field::Int(_)
            | Field::I128(_)
            | Field::Float(_)
            | Field::Boolean(_)
            | Field::Binary(_)
            | Field::Decimal(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                ">".to_string(),
            )),
        },
        Field::Timestamp(left_v) => match right_p {
            Field::Timestamp(right_v) => Ok(Field::Boolean(left_v > right_v)),
            Field::Null => Ok(Field::Null),
            Field::UInt(_)
            | Field::U128(_)
            | Field::Int(_)
            | Field::I128(_)
            | Field::Float(_)
            | Field::Boolean(_)
            | Field::String(_)
            | Field::Text(_)
            | Field::Binary(_)
            | Field::Decimal(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                ">".to_string(),
            )),
        },
        Field::Date(left_v) => match right_p {
            Field::Date(right_v) => Ok(Field::Boolean(left_v > right_v)),
            Field::Null => Ok(Field::Null),
            Field::UInt(_)
            | Field::U128(_)
            | Field::Int(_)
            | Field::I128(_)
            | Field::Float(_)
            | Field::Boolean(_)
            | Field::String(_)
            | Field::Text(_)
            | Field::Binary(_)
            | Field::Decimal(_)
            | Field::Timestamp(_)
            | Field::Bson(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                ">".to_string(),
            )),
        },
        Field::Binary(_) | Field::Bson(_) | Field::Point(_) | Field::Duration(_) => Err(
            PipelineError::InvalidTypeComparison(left_p, right_p, ">".to_string()),
        ),
    }
}

define_comparison!(evaluate_eq, "=", |l, r| { l == r });
define_comparison!(evaluate_ne, "!=", |l, r| { l != r });
define_comparison!(evaluate_lte, "<=", |l, r| { l <= r });
define_comparison!(evaluate_gte, ">=", |l, r| { l >= r });
