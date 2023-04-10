use crate::pipeline::errors::PipelineError;
use dozer_types::chrono::{DateTime, NaiveDate};
use dozer_types::rust_decimal::Decimal;
use dozer_types::types::{DozerDuration, DozerPoint, Field, Record, Schema, TimeUnit};
use num_traits::cast::*;
use std::str::FromStr;
use std::time::Duration;

pub const DATE_FORMAT: &str = "%Y-%m-%d";

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
                    // left: Bool, right: String or Text
                    Field::String(right_v) | Field::Text(right_v) => {
                        let right_v_b = bool::from_str(right_v.as_str()).map_err(|_| {
                            PipelineError::UnableToCast(format!("{}", right_v), "Bool".to_string())
                        })?;
                        Ok(Field::Boolean($function(left_v, right_v_b)))
                    }
                    // left: Bool, right: UInt
                    Field::UInt(right_v) => {
                        let right_v_b =
                            bool::from_str(right_v.to_string().as_str()).map_err(|_| {
                                PipelineError::UnableToCast(
                                    format!("{}", right_v),
                                    "Bool".to_string(),
                                )
                            })?;
                        Ok(Field::Boolean($function(left_v, right_v_b)))
                    }
                    // left: Bool, right: U128
                    Field::U128(right_v) => {
                        let right_v_b =
                            bool::from_str(right_v.to_string().as_str()).map_err(|_| {
                                PipelineError::UnableToCast(
                                    format!("{}", right_v),
                                    "Bool".to_string(),
                                )
                            })?;
                        Ok(Field::Boolean($function(left_v, right_v_b)))
                    }
                    // left: Bool, right: Int
                    Field::Int(right_v) => {
                        let right_v_b =
                            bool::from_str(right_v.to_string().as_str()).map_err(|_| {
                                PipelineError::UnableToCast(
                                    format!("{}", right_v),
                                    "Bool".to_string(),
                                )
                            })?;
                        Ok(Field::Boolean($function(left_v, right_v_b)))
                    }
                    // left: Bool, right: I128
                    Field::I128(right_v) => {
                        let right_v_b =
                            bool::from_str(right_v.to_string().as_str()).map_err(|_| {
                                PipelineError::UnableToCast(
                                    format!("{}", right_v),
                                    "Bool".to_string(),
                                )
                            })?;
                        Ok(Field::Boolean($function(left_v, right_v_b)))
                    }
                    // left: Bool, right: Float
                    Field::Float(right_v) => {
                        let right_v_b =
                            bool::from_str(right_v.to_string().as_str()).map_err(|_| {
                                PipelineError::UnableToCast(
                                    format!("{}", right_v),
                                    "Bool".to_string(),
                                )
                            })?;
                        Ok(Field::Boolean($function(left_v, right_v_b)))
                    }
                    // left: Bool, right: Decimal
                    Field::Decimal(right_v) => {
                        let right_v_b =
                            bool::from_str(right_v.to_string().as_str()).map_err(|_| {
                                PipelineError::UnableToCast(
                                    format!("{}", right_v),
                                    "Bool".to_string(),
                                )
                            })?;
                        Ok(Field::Boolean($function(left_v, right_v_b)))
                    }
                    Field::Binary(_)
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
                    Field::Float(right_v) => Ok(Field::Boolean($function(left_v as f64, *right_v))),
                    // left: Int, right: Decimal
                    Field::Decimal(right_v) => {
                        let left_v_d =
                            Decimal::from_i64(left_v).ok_or(PipelineError::UnableToCast(
                                format!("{}", left_v),
                                "Decimal".to_string(),
                            ))?;
                        Ok(Field::Boolean($function(left_v_d, right_v)))
                    }
                    // left: Int, right: String or Text
                    Field::String(right_v) | Field::Text(right_v) => {
                        let right_v_b = i64::from_str(right_v.as_str()).map_err(|_| {
                            PipelineError::UnableToCast(format!("{}", right_v), "Int".to_string())
                        })?;
                        Ok(Field::Boolean($function(left_v, right_v_b)))
                    }
                    // left: Int, right: Duration
                    Field::Duration(right_v) => {
                        let left_v_b = DozerDuration(
                            Duration::from_nanos(left_v as u64),
                            TimeUnit::Nanoseconds,
                        );
                        Ok(Field::Boolean($function(left_v_b, right_v)))
                    }
                    // left: Int, right: Null
                    Field::Null => Ok(Field::Null),
                    Field::Boolean(_)
                    | Field::Binary(_)
                    | Field::Timestamp(_)
                    | Field::Date(_)
                    | Field::Bson(_)
                    | Field::Point(_) => Err(PipelineError::InvalidTypeComparison(
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
                    Field::Float(right_v) => Ok(Field::Boolean($function(left_v as f64, *right_v))),
                    // left: I128, right: Decimal
                    Field::Decimal(right_v) => {
                        let left_v_d =
                            Decimal::from_i128(left_v).ok_or(PipelineError::UnableToCast(
                                format!("{}", left_v),
                                "Decimal".to_string(),
                            ))?;
                        Ok(Field::Boolean($function(left_v_d, right_v)))
                    }
                    // left: I128, right: String or Text
                    Field::String(right_v) | Field::Text(right_v) => {
                        let right_v_b = i128::from_str(right_v.as_str()).map_err(|_| {
                            PipelineError::UnableToCast(format!("{}", right_v), "I128".to_string())
                        })?;
                        Ok(Field::Boolean($function(left_v, right_v_b)))
                    }
                    // left: I128, right: Duration
                    Field::Duration(right_v) => {
                        let left_v_b = DozerDuration(
                            Duration::from_nanos(left_v as u64),
                            TimeUnit::Nanoseconds,
                        );
                        Ok(Field::Boolean($function(left_v_b, right_v)))
                    }
                    // left: I128, right: Null
                    Field::Null => Ok(Field::Null),
                    Field::Boolean(_)
                    | Field::Binary(_)
                    | Field::Timestamp(_)
                    | Field::Date(_)
                    | Field::Bson(_)
                    | Field::Point(_) => Err(PipelineError::InvalidTypeComparison(
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
                    Field::Float(right_v) => Ok(Field::Boolean($function(left_v as f64, *right_v))),
                    // left: UInt, right: Decimal
                    Field::Decimal(right_v) => {
                        let left_v_d =
                            Decimal::from_f64(left_v as f64).ok_or(PipelineError::UnableToCast(
                                format!("{}", left_v),
                                "Decimal".to_string(),
                            ))?;
                        Ok(Field::Boolean($function(left_v_d, right_v)))
                    }
                    // left: UInt, right: String or Text
                    Field::String(right_v) | Field::Text(right_v) => {
                        let right_v_b = u64::from_str(right_v.as_str()).map_err(|_| {
                            PipelineError::UnableToCast(format!("{}", right_v), "UInt".to_string())
                        })?;
                        Ok(Field::Boolean($function(left_v, right_v_b)))
                    }
                    // left: UInt, right: Duration
                    Field::Duration(right_v) => {
                        let left_v_b =
                            DozerDuration(Duration::from_nanos(left_v), TimeUnit::Nanoseconds);
                        Ok(Field::Boolean($function(left_v_b, right_v)))
                    }
                    // left: UInt, right: Null
                    Field::Null => Ok(Field::Null),
                    Field::Boolean(_)
                    | Field::Binary(_)
                    | Field::Timestamp(_)
                    | Field::Date(_)
                    | Field::Bson(_)
                    | Field::Point(_) => Err(PipelineError::InvalidTypeComparison(
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
                    Field::Float(right_v) => Ok(Field::Boolean($function(left_v as f64, *right_v))),
                    // left: U128, right: Decimal
                    Field::Decimal(right_v) => {
                        let left_v_d =
                            Decimal::from_f64(left_v as f64).ok_or(PipelineError::UnableToCast(
                                format!("{}", left_v),
                                "Decimal".to_string(),
                            ))?;
                        Ok(Field::Boolean($function(left_v_d, right_v)))
                    }
                    // left: U128, right: String or Text
                    Field::String(right_v) | Field::Text(right_v) => {
                        let right_v_b = u128::from_str(right_v.as_str()).map_err(|_| {
                            PipelineError::UnableToCast(format!("{}", right_v), "U128".to_string())
                        })?;
                        Ok(Field::Boolean($function(left_v, right_v_b)))
                    }
                    // left: U128, right: Duration
                    Field::Duration(right_v) => {
                        let left_v_b = DozerDuration(
                            Duration::from_nanos(left_v as u64),
                            TimeUnit::Nanoseconds,
                        );
                        Ok(Field::Boolean($function(left_v_b, right_v)))
                    }
                    // left: U128, right: Null
                    Field::Null => Ok(Field::Null),
                    Field::Boolean(_)
                    | Field::Binary(_)
                    | Field::Timestamp(_)
                    | Field::Date(_)
                    | Field::Bson(_)
                    | Field::Point(_) => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::Float(left_v) => match right_p {
                    // left: Float, right: Float
                    Field::Float(right_v) => Ok(Field::Boolean($function(left_v, right_v))),
                    // left: Float, right: UInt
                    Field::UInt(right_v) => Ok(Field::Boolean($function(*left_v, right_v as f64))),
                    // left: Float, right: U128
                    Field::U128(right_v) => Ok(Field::Boolean($function(*left_v, right_v as f64))),
                    // left: Float, right: Int
                    Field::Int(right_v) => Ok(Field::Boolean($function(*left_v, right_v as f64))),
                    // left: Float, right: I128
                    Field::I128(right_v) => Ok(Field::Boolean($function(*left_v, right_v as f64))),
                    // left: Float, right: Decimal
                    Field::Decimal(right_v) => {
                        let left_v_d =
                            Decimal::from_f64(*left_v).ok_or(PipelineError::UnableToCast(
                                format!("{}", left_v),
                                "Decimal".to_string(),
                            ))?;
                        Ok(Field::Boolean($function(left_v_d, right_v)))
                    }
                    // left: Float, right: String or Text
                    Field::String(right_v) | Field::Text(right_v) => {
                        let right_v_b = f64::from_str(right_v.as_str()).map_err(|_| {
                            PipelineError::UnableToCast(format!("{}", right_v), "Float".to_string())
                        })?;
                        Ok(Field::Boolean($function(*left_v, right_v_b)))
                    }
                    // left: Float, right: Null
                    Field::Null => Ok(Field::Null),
                    Field::Boolean(_)
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
                    // left: Decimal, right: String or Text
                    Field::String(right_v) | Field::Text(right_v) => {
                        let right_v_b = Decimal::from_str(right_v.as_str()).map_err(|_| {
                            PipelineError::UnableToCast(
                                format!("{}", right_v),
                                "Decimal".to_string(),
                            )
                        })?;
                        Ok(Field::Boolean($function(left_v, right_v_b)))
                    }
                    // left: Decimal, right: Null
                    Field::Null => Ok(Field::Null),
                    Field::Boolean(_)
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
                Field::String(ref left_v) | Field::Text(ref left_v) => match right_p {
                    Field::String(ref right_v) => Ok(Field::Boolean($function(left_v, right_v))),
                    Field::Text(ref right_v) => Ok(Field::Boolean($function(left_v, right_v))),
                    Field::Null => Ok(Field::Null),
                    Field::UInt(right_v) => {
                        let left_val = u64::from_str(left_v).map_err(|_| {
                            PipelineError::UnableToCast(format!("{}", left_v), "UInt".to_string())
                        })?;
                        Ok(Field::Boolean($function(left_val, right_v)))
                    }
                    Field::U128(right_v) => {
                        let left_val = u128::from_str(left_v).map_err(|_| {
                            PipelineError::UnableToCast(format!("{}", left_v), "U128".to_string())
                        })?;
                        Ok(Field::Boolean($function(left_val, right_v)))
                    }
                    Field::Int(right_v) => {
                        let left_val = i64::from_str(left_v).map_err(|_| {
                            PipelineError::UnableToCast(format!("{}", left_v), "Int".to_string())
                        })?;
                        Ok(Field::Boolean($function(left_val, right_v)))
                    }
                    Field::I128(right_v) => {
                        let left_val = i128::from_str(left_v).map_err(|_| {
                            PipelineError::UnableToCast(format!("{}", left_v), "I128".to_string())
                        })?;
                        Ok(Field::Boolean($function(left_val, right_v)))
                    }
                    Field::Float(right_v) => {
                        let left_val = f64::from_str(left_v).map_err(|_| {
                            PipelineError::UnableToCast(format!("{}", left_v), "Float".to_string())
                        })?;
                        Ok(Field::Boolean($function(left_val, *right_v)))
                    }
                    Field::Boolean(right_v) => {
                        let left_val = bool::from_str(left_v).map_err(|_| {
                            PipelineError::UnableToCast(format!("{}", left_v), "Bool".to_string())
                        })?;
                        Ok(Field::Boolean($function(left_val, right_v)))
                    }
                    Field::Decimal(right_v) => {
                        let left_val = Decimal::from_str(left_v).map_err(|_| {
                            PipelineError::UnableToCast(
                                format!("{}", left_v),
                                "Decimal".to_string(),
                            )
                        })?;
                        Ok(Field::Boolean($function(left_val, right_v)))
                    }
                    Field::Timestamp(right_v) => {
                        let ts = DateTime::parse_from_rfc3339(left_v).map_err(|_| {
                            PipelineError::UnableToCast(
                                format!("{}", left_v),
                                "Timestamp".to_string(),
                            )
                        })?;
                        Ok(Field::Boolean($function(ts, right_v)))
                    }
                    Field::Date(right_v) => {
                        let date =
                            NaiveDate::parse_from_str(left_v, DATE_FORMAT).map_err(|_| {
                                PipelineError::UnableToCast(
                                    format!("{}", left_v),
                                    "Date".to_string(),
                                )
                            })?;
                        Ok(Field::Boolean($function(date, right_v)))
                    }
                    Field::Point(right_v) => {
                        let left_val = DozerPoint::from_str(left_v).map_err(|_| {
                            PipelineError::UnableToCast(format!("{}", left_v), "Point".to_string())
                        })?;
                        Ok(Field::Boolean($function(left_val, right_v)))
                    }
                    Field::Duration(right_v) => {
                        let left_val = DozerDuration::from_str(left_v).map_err(|_| {
                            PipelineError::UnableToCast(
                                format!("{}", left_v),
                                "Duration".to_string(),
                            )
                        })?;
                        Ok(Field::Boolean($function(left_val, right_v)))
                    }
                    Field::Binary(_) | Field::Bson(_) => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::Timestamp(left_v) => match right_p {
                    Field::Timestamp(right_v) => Ok(Field::Boolean($function(left_v, right_v))),
                    Field::String(ref right_v) | Field::Text(ref right_v) => {
                        let ts = DateTime::parse_from_rfc3339(right_v).map_err(|_| {
                            PipelineError::UnableToCast(
                                format!("{}", right_v),
                                "Timestamp".to_string(),
                            )
                        })?;
                        Ok(Field::Boolean($function(left_v, ts)))
                    }
                    Field::Null => Ok(Field::Null),
                    Field::UInt(_)
                    | Field::U128(_)
                    | Field::Int(_)
                    | Field::I128(_)
                    | Field::Float(_)
                    | Field::Boolean(_)
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
                    Field::String(ref right_v) | Field::Text(ref right_v) => {
                        let date =
                            NaiveDate::parse_from_str(right_v, DATE_FORMAT).map_err(|_| {
                                PipelineError::UnableToCast(
                                    format!("{}", right_v),
                                    "Date".to_string(),
                                )
                            })?;
                        Ok(Field::Boolean($function(left_v, date)))
                    }
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
                    | Field::Bson(_)
                    | Field::Point(_)
                    | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::Point(left_v) => match right_p {
                    Field::Point(right_v) => Ok(Field::Boolean($function(left_v, right_v))),
                    Field::String(right_v) | Field::Text(right_v) => {
                        let right_val = DozerPoint::from_str(right_v.as_str()).map_err(|_| {
                            PipelineError::UnableToCast(format!("{}", right_v), "Point".to_string())
                        })?;
                        Ok(Field::Boolean($function(left_v, right_val)))
                    }
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
                    | Field::Bson(_)
                    | Field::Date(_)
                    | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::Duration(left_v) => match right_p {
                    Field::Duration(right_v) => Ok(Field::Boolean($function(left_v, right_v))),
                    Field::String(right_v) | Field::Text(right_v) => {
                        let right_val =
                            DozerDuration::from_str(right_v.as_str()).map_err(|_| {
                                PipelineError::UnableToCast(
                                    format!("{}", right_v),
                                    "Duration".to_string(),
                                )
                            })?;
                        Ok(Field::Boolean($function(left_v, right_val)))
                    }
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
                    | Field::Bson(_)
                    | Field::Date(_)
                    | Field::Point(_) => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::Binary(_) | Field::Bson(_) => Err(PipelineError::InvalidTypeComparison(
                    left_p,
                    right_p,
                    $op.to_string(),
                )),
            }
        }
    };
}

use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};

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
            // left: Bool, right: String or Text
            Field::String(right_v) | Field::Text(right_v) => {
                let right_v_b = bool::from_str(right_v.as_str()).map_err(|_| {
                    PipelineError::UnableToCast(right_v.to_string(), "Bool".to_string())
                })?;
                Ok(Field::Boolean(!left_v & right_v_b))
            }
            // left: Bool, right: UInt
            Field::UInt(right_v) => {
                let right_v_b = bool::from_str(right_v.to_string().as_str()).map_err(|_| {
                    PipelineError::UnableToCast(format!("{}", right_v), "Bool".to_string())
                })?;
                Ok(Field::Boolean(!left_v & right_v_b))
            }
            // left: Bool, right: U128
            Field::U128(right_v) => {
                let right_v_b = bool::from_str(right_v.to_string().as_str()).map_err(|_| {
                    PipelineError::UnableToCast(format!("{}", right_v), "Bool".to_string())
                })?;
                Ok(Field::Boolean(!left_v & right_v_b))
            }
            // left: Bool, right: Int
            Field::Int(right_v) => {
                let right_v_b = bool::from_str(right_v.to_string().as_str()).map_err(|_| {
                    PipelineError::UnableToCast(format!("{}", right_v), "Bool".to_string())
                })?;
                Ok(Field::Boolean(!left_v & right_v_b))
            }
            // left: Bool, right: I128
            Field::I128(right_v) => {
                let right_v_b = bool::from_str(right_v.to_string().as_str()).map_err(|_| {
                    PipelineError::UnableToCast(format!("{}", right_v), "Bool".to_string())
                })?;
                Ok(Field::Boolean(!left_v & right_v_b))
            }
            // left: Bool, right: Float
            Field::Float(right_v) => {
                let right_v_b = bool::from_str(right_v.to_string().as_str()).map_err(|_| {
                    PipelineError::UnableToCast(format!("{}", right_v), "Bool".to_string())
                })?;
                Ok(Field::Boolean(!left_v & right_v_b))
            }
            // left: Bool, right: Decimal
            Field::Decimal(right_v) => {
                let right_v_b = bool::from_str(right_v.to_string().as_str()).map_err(|_| {
                    PipelineError::UnableToCast(format!("{}", right_v), "Bool".to_string())
                })?;
                Ok(Field::Boolean(!left_v & right_v_b))
            }
            Field::Binary(_)
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
            Field::Float(right_v) => Ok(Field::Boolean((left_v as f64) < *right_v)),
            // left: Int, right: Decimal
            Field::Decimal(right_v) => {
                let left_v_d = Decimal::from_i64(left_v).ok_or(PipelineError::UnableToCast(
                    format!("{}", left_v),
                    "Decimal".to_string(),
                ))?;
                Ok(Field::Boolean(left_v_d < right_v))
            }
            // left: Int, right: String or Text
            Field::String(right_v) | Field::Text(right_v) => {
                let right_v_b = i64::from_str(right_v.as_str()).map_err(|_| {
                    PipelineError::UnableToCast(right_v.to_string(), "Int".to_string())
                })?;
                Ok(Field::Boolean(left_v < right_v_b))
            }
            // left: Int, right: Duration
            Field::Duration(right_v) => {
                let left_v_b =
                    DozerDuration(Duration::from_nanos(left_v as u64), TimeUnit::Nanoseconds);
                Ok(Field::Boolean(left_v_b < right_v))
            }
            // left: Int, right: Null
            Field::Null => Ok(Field::Null),
            Field::Boolean(_)
            | Field::Binary(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_) => Err(PipelineError::InvalidTypeComparison(
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
            Field::Float(right_v) => Ok(Field::Boolean((left_v as f64) < *right_v)),
            // left: I128, right: Decimal
            Field::Decimal(right_v) => {
                let left_v_d = Decimal::from_i128(left_v).ok_or(PipelineError::UnableToCast(
                    format!("{}", left_v),
                    "Decimal".to_string(),
                ))?;
                Ok(Field::Boolean(left_v_d < right_v))
            }
            // left: I128, right: String or Text
            Field::String(right_v) | Field::Text(right_v) => {
                let right_v_b = i128::from_str(right_v.as_str()).map_err(|_| {
                    PipelineError::UnableToCast(right_v.to_string(), "I128".to_string())
                })?;
                Ok(Field::Boolean(left_v < right_v_b))
            }
            // left: I128, right: Duration
            Field::Duration(right_v) => {
                let left_v_b =
                    DozerDuration(Duration::from_nanos(left_v as u64), TimeUnit::Nanoseconds);
                Ok(Field::Boolean(left_v_b < right_v))
            }
            // left: I128, right: Null
            Field::Null => Ok(Field::Null),
            Field::Boolean(_)
            | Field::Binary(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_) => Err(PipelineError::InvalidTypeComparison(
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
            Field::Float(right_v) => Ok(Field::Boolean((left_v as f64) < *right_v)),
            // left: UInt, right: Decimal
            Field::Decimal(right_v) => {
                let left_v_d = Decimal::from_f64(left_v as f64).ok_or(
                    PipelineError::UnableToCast(format!("{}", left_v), "Decimal".to_string()),
                )?;
                Ok(Field::Boolean(left_v_d < right_v))
            }
            // left: UInt, right: String or Text
            Field::String(right_v) | Field::Text(right_v) => {
                let right_v_b = u64::from_str(right_v.as_str()).map_err(|_| {
                    PipelineError::UnableToCast(right_v.to_string(), "UInt".to_string())
                })?;
                Ok(Field::Boolean(left_v < right_v_b))
            }
            // left: UInt, right: Duration
            Field::Duration(right_v) => {
                let left_v_b = DozerDuration(Duration::from_nanos(left_v), TimeUnit::Nanoseconds);
                Ok(Field::Boolean(left_v_b < right_v))
            }
            // left: UInt, right: Null
            Field::Null => Ok(Field::Null),
            Field::Boolean(_)
            | Field::Binary(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_) => Err(PipelineError::InvalidTypeComparison(
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
            Field::Float(right_v) => Ok(Field::Boolean((left_v as f64) < *right_v)),
            // left: U128, right: Decimal
            Field::Decimal(right_v) => {
                let left_v_d = Decimal::from_f64(left_v as f64).ok_or(
                    PipelineError::UnableToCast(format!("{}", left_v), "Decimal".to_string()),
                )?;
                Ok(Field::Boolean(left_v_d < right_v))
            }
            // left: U128, right: String or Text
            Field::String(right_v) | Field::Text(right_v) => {
                let right_v_b = u128::from_str(right_v.as_str()).map_err(|_| {
                    PipelineError::UnableToCast(right_v.to_string(), "U128".to_string())
                })?;
                Ok(Field::Boolean(left_v < right_v_b))
            }
            // left: U128, right: Duration
            Field::Duration(right_v) => {
                let left_v_b =
                    DozerDuration(Duration::from_nanos(left_v as u64), TimeUnit::Nanoseconds);
                Ok(Field::Boolean(left_v_b < right_v))
            }
            // left: U128, right: Null
            Field::Null => Ok(Field::Null),
            Field::Boolean(_)
            | Field::Binary(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                "<".to_string(),
            )),
        },
        Field::Float(left_v) => match right_p {
            // left: Float, right: Float
            Field::Float(right_v) => Ok(Field::Boolean(left_v < right_v)),
            // left: Float, right: UInt
            Field::UInt(right_v) => Ok(Field::Boolean(*left_v < (right_v as f64))),
            // left: Float, right: U128
            Field::U128(right_v) => Ok(Field::Boolean(*left_v < (right_v as f64))),
            // left: Float, right: Int
            Field::Int(right_v) => Ok(Field::Boolean(*left_v < (right_v as f64))),
            // left: Float, right: I128
            Field::I128(right_v) => Ok(Field::Boolean(*left_v < (right_v as f64))),
            // left: Float, right: Decimal
            Field::Decimal(right_v) => {
                let left_v_d = Decimal::from_f64(*left_v).ok_or(PipelineError::UnableToCast(
                    format!("{}", left_v),
                    "Decimal".to_string(),
                ))?;
                Ok(Field::Boolean(left_v_d < right_v))
            }
            // left: Float, right: String or Text
            Field::String(right_v) | Field::Text(right_v) => {
                let right_v_b = f64::from_str(right_v.as_str()).map_err(|_| {
                    PipelineError::UnableToCast(right_v.to_string(), "Float".to_string())
                })?;
                Ok(Field::Boolean(*left_v < right_v_b))
            }
            // left: Float, right: Null
            Field::Null => Ok(Field::Null),
            Field::Boolean(_)
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
                // left: Decimal, right: String or Text
                Field::String(right_v) | Field::Text(right_v) => {
                    let right_v_b = Decimal::from_str(right_v.as_str()).map_err(|_| {
                        PipelineError::UnableToCast(right_v.to_string(), "Decimal".to_string())
                    })?;
                    Ok(Field::Boolean(left_v < right_v_b))
                }
                // left: Decimal, right: Null
                Field::Null => Ok(Field::Null),
                Field::Boolean(_)
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
        Field::String(ref left_v) | Field::Text(ref left_v) => match right_p {
            Field::String(ref right_v) => Ok(Field::Boolean(left_v < right_v)),
            Field::Text(ref right_v) => Ok(Field::Boolean(left_v < right_v)),
            Field::Null => Ok(Field::Null),
            Field::UInt(right_v) => {
                let left_val = u64::from_str(left_v).map_err(|_| {
                    PipelineError::UnableToCast(left_v.to_string(), "UInt".to_string())
                })?;
                Ok(Field::Boolean(left_val < right_v))
            }
            Field::U128(right_v) => {
                let left_val = u128::from_str(left_v).map_err(|_| {
                    PipelineError::UnableToCast(left_v.to_string(), "U128".to_string())
                })?;
                Ok(Field::Boolean(left_val < right_v))
            }
            Field::Int(right_v) => {
                let left_val = i64::from_str(left_v).map_err(|_| {
                    PipelineError::UnableToCast(left_v.to_string(), "Int".to_string())
                })?;
                Ok(Field::Boolean(left_val < right_v))
            }
            Field::I128(right_v) => {
                let left_val = i128::from_str(left_v).map_err(|_| {
                    PipelineError::UnableToCast(left_v.to_string(), "I128".to_string())
                })?;
                Ok(Field::Boolean(left_val < right_v))
            }
            Field::Float(right_v) => {
                let left_val = f64::from_str(left_v).map_err(|_| {
                    PipelineError::UnableToCast(left_v.to_string(), "Float".to_string())
                })?;
                Ok(Field::Boolean(left_val < *right_v))
            }
            Field::Boolean(right_v) => {
                let left_val = bool::from_str(left_v).map_err(|_| {
                    PipelineError::UnableToCast(left_v.to_string(), "Bool".to_string())
                })?;
                Ok(Field::Boolean(!left_val & right_v))
            }
            Field::Decimal(right_v) => {
                let left_val = Decimal::from_str(left_v).map_err(|_| {
                    PipelineError::UnableToCast(left_v.to_string(), "Decimal".to_string())
                })?;
                Ok(Field::Boolean(left_val < right_v))
            }
            Field::Timestamp(right_v) => {
                let ts = DateTime::parse_from_rfc3339(left_v).map_err(|_| {
                    PipelineError::UnableToCast(left_v.to_string(), "Timestamp".to_string())
                })?;
                Ok(Field::Boolean(ts < right_v))
            }
            Field::Date(right_v) => {
                let date = NaiveDate::parse_from_str(left_v, DATE_FORMAT).map_err(|_| {
                    PipelineError::UnableToCast(left_v.to_string(), "Date".to_string())
                })?;
                Ok(Field::Boolean(date < right_v))
            }
            Field::Point(right_v) => {
                let left_val = DozerPoint::from_str(left_v).map_err(|_| {
                    PipelineError::UnableToCast(left_v.to_string(), "Point".to_string())
                })?;
                Ok(Field::Boolean(left_val < right_v))
            }
            Field::Duration(right_v) => {
                let left_val = DozerDuration::from_str(left_v).map_err(|_| {
                    PipelineError::UnableToCast(left_v.to_string(), "Duration".to_string())
                })?;
                Ok(Field::Boolean(left_val < right_v))
            }
            Field::Binary(_) | Field::Bson(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                "<".to_string(),
            )),
        },
        Field::Timestamp(left_v) => match right_p {
            Field::Timestamp(right_v) => Ok(Field::Boolean(left_v < right_v)),
            Field::String(ref right_v) | Field::Text(ref right_v) => {
                let ts = DateTime::parse_from_rfc3339(right_v).map_err(|_| {
                    PipelineError::UnableToCast(right_v.to_string(), "Timestamp".to_string())
                })?;
                Ok(Field::Boolean(left_v < ts))
            }
            Field::Null => Ok(Field::Null),
            Field::UInt(_)
            | Field::U128(_)
            | Field::Int(_)
            | Field::I128(_)
            | Field::Float(_)
            | Field::Boolean(_)
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
            Field::String(ref right_v) | Field::Text(ref right_v) => {
                let date = NaiveDate::parse_from_str(right_v, DATE_FORMAT).map_err(|_| {
                    PipelineError::UnableToCast(right_v.to_string(), "Date".to_string())
                })?;
                Ok(Field::Boolean(left_v < date))
            }
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
            | Field::Bson(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                "<".to_string(),
            )),
        },
        Field::Point(left_v) => match right_p {
            Field::Point(right_v) => Ok(Field::Boolean(left_v < right_v)),
            Field::String(right_v) | Field::Text(right_v) => {
                let right_val = DozerPoint::from_str(right_v.as_str()).map_err(|_| {
                    PipelineError::UnableToCast(right_v.to_string(), "Point".to_string())
                })?;
                Ok(Field::Boolean(left_v < right_val))
            }
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
            | Field::Bson(_)
            | Field::Date(_)
            | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                "<".to_string(),
            )),
        },
        Field::Duration(left_v) => match right_p {
            Field::Duration(right_v) => Ok(Field::Boolean(left_v < right_v)),
            Field::String(right_v) | Field::Text(right_v) => {
                let right_val = DozerDuration::from_str(right_v.as_str()).map_err(|_| {
                    PipelineError::UnableToCast(right_v.to_string(), "Duration".to_string())
                })?;
                Ok(Field::Boolean(left_v < right_val))
            }
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
            | Field::Bson(_)
            | Field::Date(_)
            | Field::Point(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                "<".to_string(),
            )),
        },
        Field::Binary(_) | Field::Bson(_) => Err(PipelineError::InvalidTypeComparison(
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
        Field::Null => Ok(Field::Null),
        Field::Boolean(left_v) => match right_p {
            // left: Bool, right: Bool
            Field::Boolean(right_v) => Ok(Field::Boolean(left_v & !right_v)),
            Field::String(right_v) | Field::Text(right_v) => {
                let right_v_b = bool::from_str(right_v.as_str()).unwrap();
                Ok(Field::Boolean(left_v & !right_v_b))
            }
            // left: Bool, right: UInt
            Field::UInt(right_v) => {
                let right_v_b = bool::from_str(right_v.to_string().as_str()).map_err(|_| {
                    PipelineError::UnableToCast(format!("{}", right_v), "Bool".to_string())
                })?;
                Ok(Field::Boolean(left_v & !right_v_b))
            }
            // left: Bool, right: U128
            Field::U128(right_v) => {
                let right_v_b = bool::from_str(right_v.to_string().as_str()).map_err(|_| {
                    PipelineError::UnableToCast(format!("{}", right_v), "Bool".to_string())
                })?;
                Ok(Field::Boolean(left_v & !right_v_b))
            }
            // left: Bool, right: Int
            Field::Int(right_v) => {
                let right_v_b = bool::from_str(right_v.to_string().as_str()).map_err(|_| {
                    PipelineError::UnableToCast(format!("{}", right_v), "Bool".to_string())
                })?;
                Ok(Field::Boolean(left_v & !right_v_b))
            }
            // left: Bool, right: I128
            Field::I128(right_v) => {
                let right_v_b = bool::from_str(right_v.to_string().as_str()).map_err(|_| {
                    PipelineError::UnableToCast(format!("{}", right_v), "Bool".to_string())
                })?;
                Ok(Field::Boolean(left_v & !right_v_b))
            }
            // left: Bool, right: Float
            Field::Float(right_v) => {
                let right_v_b = bool::from_str(right_v.to_string().as_str()).map_err(|_| {
                    PipelineError::UnableToCast(format!("{}", right_v), "Bool".to_string())
                })?;
                Ok(Field::Boolean(left_v & !right_v_b))
            }
            // left: Bool, right: Decimal
            Field::Decimal(right_v) => {
                let right_v_b = bool::from_str(right_v.to_string().as_str()).map_err(|_| {
                    PipelineError::UnableToCast(format!("{}", right_v), "Bool".to_string())
                })?;
                Ok(Field::Boolean(left_v & !right_v_b))
            }
            Field::Binary(_)
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
            Field::Float(right_v) => Ok(Field::Boolean(left_v as f64 > *right_v)),
            // left: Int, right: Decimal
            Field::Decimal(right_v) => {
                let left_v_d = Decimal::from_i64(left_v).ok_or(PipelineError::UnableToCast(
                    format!("{}", left_v),
                    "Decimal".to_string(),
                ))?;
                Ok(Field::Boolean(left_v_d > right_v))
            }
            // left: Int, right: String or Text
            Field::String(right_v) | Field::Text(right_v) => {
                let right_v_b = i64::from_str(right_v.as_str()).map_err(|_| {
                    PipelineError::UnableToCast(right_v.to_string(), "Int".to_string())
                })?;
                Ok(Field::Boolean(left_v > right_v_b))
            }
            // left: Int, right: Duration
            Field::Duration(right_v) => {
                let left_v_b =
                    DozerDuration(Duration::from_nanos(left_v as u64), TimeUnit::Nanoseconds);
                Ok(Field::Boolean(left_v_b > right_v))
            }
            // left: Int, right: Null
            Field::Null => Ok(Field::Null),
            Field::Boolean(_)
            | Field::Binary(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_) => Err(PipelineError::InvalidTypeComparison(
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
            Field::Float(right_v) => Ok(Field::Boolean((left_v as f64) > *right_v)),
            // left: I128, right: Decimal
            Field::Decimal(right_v) => {
                let left_v_d = Decimal::from_i128(left_v).ok_or(PipelineError::UnableToCast(
                    format!("{}", left_v),
                    "Decimal".to_string(),
                ))?;
                Ok(Field::Boolean(left_v_d > right_v))
            }
            // left: I128, right: String or Text
            Field::String(right_v) | Field::Text(right_v) => {
                let right_v_b = i128::from_str(right_v.as_str()).map_err(|_| {
                    PipelineError::UnableToCast(right_v.to_string(), "I128".to_string())
                })?;
                Ok(Field::Boolean(left_v > right_v_b))
            }
            // left: I128, right: Duration
            Field::Duration(right_v) => {
                let left_v_b =
                    DozerDuration(Duration::from_nanos(left_v as u64), TimeUnit::Nanoseconds);
                Ok(Field::Boolean(left_v_b > right_v))
            }
            // left: I128, right: Null
            Field::Null => Ok(Field::Null),
            Field::Boolean(_)
            | Field::Binary(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_) => Err(PipelineError::InvalidTypeComparison(
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
            Field::Float(right_v) => Ok(Field::Boolean((left_v as f64) > *right_v)),
            // left: UInt, right: Decimal
            Field::Decimal(right_v) => {
                let left_v_d = Decimal::from_f64(left_v as f64).ok_or(
                    PipelineError::UnableToCast(format!("{}", left_v), "Decimal".to_string()),
                )?;
                Ok(Field::Boolean(left_v_d > right_v))
            }
            // left: UInt, right: String or Text
            Field::String(right_v) | Field::Text(right_v) => {
                let right_v_b = u64::from_str(right_v.as_str()).map_err(|_| {
                    PipelineError::UnableToCast(right_v.to_string(), "UInt".to_string())
                })?;
                Ok(Field::Boolean(left_v > right_v_b))
            }
            // left: UInt, right: Duration
            Field::Duration(right_v) => {
                let left_v_b = DozerDuration(Duration::from_nanos(left_v), TimeUnit::Nanoseconds);
                Ok(Field::Boolean(left_v_b > right_v))
            }
            // left: UInt, right: Null
            Field::Null => Ok(Field::Null),
            Field::Boolean(_)
            | Field::Binary(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_) => Err(PipelineError::InvalidTypeComparison(
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
            Field::Float(right_v) => Ok(Field::Boolean((left_v as f64) > *right_v)),
            // left: U128, right: Decimal
            Field::Decimal(right_v) => {
                let left_v_d = Decimal::from_f64(left_v as f64).ok_or(
                    PipelineError::UnableToCast(format!("{}", left_v), "Decimal".to_string()),
                )?;
                Ok(Field::Boolean(left_v_d > right_v))
            }
            // left: U128, right: String or Text
            Field::String(right_v) | Field::Text(right_v) => {
                let right_v_b = u128::from_str(right_v.as_str()).map_err(|_| {
                    PipelineError::UnableToCast(right_v.to_string(), "U128".to_string())
                })?;
                Ok(Field::Boolean(left_v > right_v_b))
            }
            // left: U128, right: Duration
            Field::Duration(right_v) => {
                let left_v_b =
                    DozerDuration(Duration::from_nanos(left_v as u64), TimeUnit::Nanoseconds);
                Ok(Field::Boolean(left_v_b > right_v))
            }
            // left: U128, right: Null
            Field::Null => Ok(Field::Null),
            Field::Boolean(_)
            | Field::Binary(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                ">".to_string(),
            )),
        },
        Field::Float(left_v) => match right_p {
            // left: Float, right: Float
            Field::Float(right_v) => Ok(Field::Boolean(left_v > right_v)),
            // left: Float, right: UInt
            Field::UInt(right_v) => Ok(Field::Boolean(*left_v > right_v as f64)),
            // left: Float, right: U128
            Field::U128(right_v) => Ok(Field::Boolean(*left_v > right_v as f64)),
            // left: Float, right: Int
            Field::Int(right_v) => Ok(Field::Boolean(*left_v > right_v as f64)),
            // left: Float, right: I128
            Field::I128(right_v) => Ok(Field::Boolean(*left_v > right_v as f64)),
            // left: Float, right: Decimal
            Field::Decimal(right_v) => {
                let left_v_d = Decimal::from_f64(*left_v).ok_or(PipelineError::UnableToCast(
                    format!("{}", left_v),
                    "Decimal".to_string(),
                ))?;
                Ok(Field::Boolean(left_v_d > right_v))
            }
            // left: Float, right: String or Text
            Field::String(right_v) | Field::Text(right_v) => {
                let right_v_b = f64::from_str(right_v.as_str()).map_err(|_| {
                    PipelineError::UnableToCast(right_v.to_string(), "Float".to_string())
                })?;
                Ok(Field::Boolean(*left_v > right_v_b))
            }
            // left: Float, right: Null
            Field::Null => Ok(Field::Null),
            Field::Boolean(_)
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
                // left: Decimal, right: String or Text
                Field::String(right_v) | Field::Text(right_v) => {
                    let right_v_b = Decimal::from_str(right_v.as_str()).map_err(|_| {
                        PipelineError::UnableToCast(right_v.to_string(), "Decimal".to_string())
                    })?;
                    Ok(Field::Boolean(left_v > right_v_b))
                }
                // left: Decimal, right: Null
                Field::Null => Ok(Field::Null),
                Field::Boolean(_)
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
        Field::String(ref left_v) | Field::Text(ref left_v) => match right_p {
            Field::String(ref right_v) => Ok(Field::Boolean(left_v > right_v)),
            Field::Text(ref right_v) => Ok(Field::Boolean(left_v > right_v)),
            Field::Null => Ok(Field::Null),
            Field::UInt(right_v) => {
                let left_val = u64::from_str(left_v).map_err(|_| {
                    PipelineError::UnableToCast(left_v.to_string(), "UInt".to_string())
                })?;
                Ok(Field::Boolean(left_val > right_v))
            }
            Field::U128(right_v) => {
                let left_val = u128::from_str(left_v).map_err(|_| {
                    PipelineError::UnableToCast(left_v.to_string(), "U128".to_string())
                })?;
                Ok(Field::Boolean(left_val > right_v))
            }
            Field::Int(right_v) => {
                let left_val = i64::from_str(left_v).map_err(|_| {
                    PipelineError::UnableToCast(left_v.to_string(), "Int".to_string())
                })?;
                Ok(Field::Boolean(left_val > right_v))
            }
            Field::I128(right_v) => {
                let left_val = i128::from_str(left_v).map_err(|_| {
                    PipelineError::UnableToCast(left_v.to_string(), "I128".to_string())
                })?;
                Ok(Field::Boolean(left_val > right_v))
            }
            Field::Float(right_v) => {
                let left_val = f64::from_str(left_v).map_err(|_| {
                    PipelineError::UnableToCast(left_v.to_string(), "Float".to_string())
                })?;
                Ok(Field::Boolean(left_val > *right_v))
            }
            Field::Boolean(right_v) => {
                let left_val = bool::from_str(left_v).map_err(|_| {
                    PipelineError::UnableToCast(left_v.to_string(), "Bool".to_string())
                })?;
                Ok(Field::Boolean(left_val & !right_v))
            }
            Field::Decimal(right_v) => {
                let left_val = Decimal::from_str(left_v).map_err(|_| {
                    PipelineError::UnableToCast(left_v.to_string(), "Decimal".to_string())
                })?;
                Ok(Field::Boolean(left_val > right_v))
            }
            Field::Timestamp(right_v) => {
                let ts = DateTime::parse_from_rfc3339(left_v).map_err(|_| {
                    PipelineError::UnableToCast(left_v.to_string(), "Timestamp".to_string())
                })?;
                Ok(Field::Boolean(ts > right_v))
            }
            Field::Date(right_v) => {
                let date = NaiveDate::parse_from_str(left_v, DATE_FORMAT).map_err(|_| {
                    PipelineError::UnableToCast(left_v.to_string(), "Date".to_string())
                })?;
                Ok(Field::Boolean(date > right_v))
            }
            Field::Point(right_v) => {
                let left_val = DozerPoint::from_str(left_v).map_err(|_| {
                    PipelineError::UnableToCast(left_v.to_string(), "Point".to_string())
                })?;
                Ok(Field::Boolean(left_val > right_v))
            }
            Field::Duration(right_v) => {
                let left_val = DozerDuration::from_str(left_v).map_err(|_| {
                    PipelineError::UnableToCast(left_v.to_string(), "Duration".to_string())
                })?;
                Ok(Field::Boolean(left_val > right_v))
            }
            Field::Binary(_) | Field::Bson(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                ">".to_string(),
            )),
        },
        Field::Timestamp(left_v) => match right_p {
            Field::Timestamp(right_v) => Ok(Field::Boolean(left_v > right_v)),
            Field::String(ref right_v) | Field::Text(ref right_v) => {
                let ts = DateTime::parse_from_rfc3339(right_v).map_err(|_| {
                    PipelineError::UnableToCast(right_v.to_string(), "Timestamp".to_string())
                })?;
                Ok(Field::Boolean(left_v > ts))
            }
            Field::Null => Ok(Field::Null),
            Field::UInt(_)
            | Field::U128(_)
            | Field::Int(_)
            | Field::I128(_)
            | Field::Float(_)
            | Field::Boolean(_)
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
            Field::String(ref right_v) | Field::Text(ref right_v) => {
                let date = NaiveDate::parse_from_str(right_v, DATE_FORMAT).map_err(|_| {
                    PipelineError::UnableToCast(right_v.to_string(), "Date".to_string())
                })?;
                Ok(Field::Boolean(left_v > date))
            }
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
            | Field::Bson(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                ">".to_string(),
            )),
        },
        Field::Point(left_v) => match right_p {
            Field::Point(right_v) => Ok(Field::Boolean(left_v > right_v)),
            Field::String(right_v) | Field::Text(right_v) => {
                let right_val = DozerPoint::from_str(right_v.as_str()).map_err(|_| {
                    PipelineError::UnableToCast(right_v.to_string(), "Point".to_string())
                })?;
                Ok(Field::Boolean(left_v > right_val))
            }
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
            | Field::Bson(_)
            | Field::Date(_)
            | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                ">".to_string(),
            )),
        },
        Field::Duration(left_v) => match right_p {
            Field::Duration(right_v) => Ok(Field::Boolean(left_v > right_v)),
            Field::String(right_v) | Field::Text(right_v) => {
                let right_val = DozerDuration::from_str(right_v.as_str()).map_err(|_| {
                    PipelineError::UnableToCast(right_v.to_string(), "Duration".to_string())
                })?;
                Ok(Field::Boolean(left_v > right_val))
            }
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
            | Field::Bson(_)
            | Field::Date(_)
            | Field::Point(_) => Err(PipelineError::InvalidTypeComparison(
                left_p,
                right_p,
                ">".to_string(),
            )),
        },
        Field::Binary(_) | Field::Bson(_) => Err(PipelineError::InvalidTypeComparison(
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
