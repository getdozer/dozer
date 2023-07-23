use crate::pipeline::errors::OperationError;
use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::SqlError::Operation;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use dozer_types::rust_decimal::Decimal;
use dozer_types::types::Schema;
use dozer_types::types::{DozerDuration, TimeUnit};
use dozer_types::{
    chrono,
    ordered_float::OrderedFloat,
    types::{Field, ProcessorRecord},
};
use num_traits::{FromPrimitive, Zero};
use std::num::Wrapping;
use std::ops::Neg;

macro_rules! define_math_operator {
    ($id:ident, $op:expr, $fct:expr, $t: expr) => {
        pub fn $id(
            schema: &Schema,
            left: &Expression,
            right: &Expression,
            record: &Record,
        ) -> Result<Field, PipelineError> {
            let left_p = left.evaluate(&record, schema)?;
            let right_p = right.evaluate(&record, schema)?;

            match left_p {
                Field::Duration(left_v) => {
                    match right_p {
                        Field::Duration(right_v) => match $op {
                            "-" => {
                                let duration = left_v.0.checked_sub(right_v.0).ok_or(
                                    PipelineError::SqlError(Operation(
                                        OperationError::AdditionOverflow,
                                    )),
                                )?;
                                Ok(Field::from(DozerDuration(duration, TimeUnit::Nanoseconds)))
                            }
                            "+" => {
                                let duration = left_v.0.checked_add(right_v.0).ok_or(
                                    PipelineError::SqlError(Operation(
                                        OperationError::SubtractionOverflow,
                                    )),
                                )?;
                                Ok(Field::from(DozerDuration(duration, TimeUnit::Nanoseconds)))
                            }
                            "*" | "/" | "%" => Err(PipelineError::InvalidTypeComparison(
                                left_p,
                                right_p,
                                $op.to_string(),
                            )),
                            &_ => Err(PipelineError::InvalidTypeComparison(
                                left_p,
                                right_p,
                                $op.to_string(),
                            )),
                        },
                        Field::Timestamp(right_v) => match $op {
                            "+" => {
                                let duration = right_v
                                    .checked_add_signed(chrono::Duration::nanoseconds(
                                        left_v.0.as_nanos() as i64,
                                    ))
                                    .ok_or(PipelineError::SqlError(Operation(
                                        OperationError::AdditionOverflow,
                                    )))?;
                                Ok(Field::Timestamp(duration))
                            }
                            "-" | "*" | "/" | "%" => Err(PipelineError::InvalidTypeComparison(
                                left_p,
                                right_p,
                                $op.to_string(),
                            )),
                            &_ => Err(PipelineError::InvalidTypeComparison(
                                left_p,
                                right_p,
                                $op.to_string(),
                            )),
                        },
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
                        | Field::Json(_)
                        | Field::Point(_)
                        | Field::Null => Err(PipelineError::InvalidTypeComparison(
                            left_p,
                            right_p,
                            $op.to_string(),
                        )),
                    }
                }
                Field::Timestamp(left_v) => match right_p {
                    Field::Duration(right_v) => match $op {
                        "-" => {
                            let duration = left_v
                                .checked_sub_signed(chrono::Duration::nanoseconds(
                                    right_v.0.as_nanos() as i64,
                                ))
                                .ok_or(PipelineError::SqlError(Operation(
                                    OperationError::AdditionOverflow,
                                )))?;
                            Ok(Field::Timestamp(duration))
                        }
                        "+" => {
                            let duration = left_v
                                .checked_add_signed(chrono::Duration::nanoseconds(
                                    right_v.0.as_nanos() as i64,
                                ))
                                .ok_or(PipelineError::SqlError(Operation(
                                    OperationError::SubtractionOverflow,
                                )))?;
                            Ok(Field::Timestamp(duration))
                        }
                        "*" | "/" | "%" => Err(PipelineError::InvalidTypeComparison(
                            left_p,
                            right_p,
                            $op.to_string(),
                        )),
                        &_ => Err(PipelineError::InvalidTypeComparison(
                            left_p,
                            right_p,
                            $op.to_string(),
                        )),
                    },
                    Field::Timestamp(right_v) => match $op {
                        "-" => {
                            if left_v > right_v {
                                let duration: i64 = (left_v - right_v).num_nanoseconds().ok_or(
                                    PipelineError::UnableToCast(
                                        format!("{}", left_v - right_v),
                                        "i64".to_string(),
                                    ),
                                )?;
                                Ok(Field::from(DozerDuration(
                                    std::time::Duration::from_nanos(duration as u64),
                                    TimeUnit::Nanoseconds,
                                )))
                            } else {
                                Err(PipelineError::InvalidTypeComparison(
                                    left_p,
                                    right_p,
                                    $op.to_string(),
                                ))
                            }
                        }
                        "+" | "*" | "/" | "%" => Err(PipelineError::InvalidTypeComparison(
                            left_p,
                            right_p,
                            $op.to_string(),
                        )),
                        &_ => Err(PipelineError::InvalidTypeComparison(
                            left_p,
                            right_p,
                            $op.to_string(),
                        )),
                    },
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
                    | Field::Json(_)
                    | Field::Point(_)
                    | Field::Null => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::Float(left_v) => match right_p {
                    // left: Float, right: Int
                    Field::Int(right_v) => {
                        return match $op {
                            "/" | "%" => {
                                if right_v == 0_i64 {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Float($fct(
                                        left_v,
                                        OrderedFloat::<f64>::from_i64(right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", right_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                    )))
                                }
                            }
                            &_ => Ok(Field::Float($fct(
                                left_v,
                                OrderedFloat::<f64>::from_i64(right_v).ok_or(
                                    PipelineError::UnableToCast(
                                        format!("{}", right_v),
                                        "f64".to_string(),
                                    ),
                                )?,
                            ))),
                        }
                    }
                    // left: Float, right: I128
                    Field::I128(right_v) => {
                        return match $op {
                            "/" | "%" => {
                                if right_v == 0_i128 {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Float($fct(
                                        left_v,
                                        OrderedFloat::<f64>::from_i128(right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", right_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                    )))
                                }
                            }
                            &_ => Ok(Field::Float($fct(
                                left_v,
                                OrderedFloat::<f64>::from_i128(right_v).ok_or(
                                    PipelineError::UnableToCast(
                                        format!("{}", right_v),
                                        "f64".to_string(),
                                    ),
                                )?,
                            ))),
                        }
                    }
                    // left: Float, right: UInt
                    Field::UInt(right_v) => {
                        return match $op {
                            "/" | "%" => {
                                if right_v == 0_u64 {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Float($fct(
                                        left_v,
                                        OrderedFloat::<f64>::from_u64(right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", right_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                    )))
                                }
                            }
                            &_ => Ok(Field::Float($fct(
                                left_v,
                                OrderedFloat::<f64>::from_u64(right_v).ok_or(
                                    PipelineError::UnableToCast(
                                        format!("{}", right_v),
                                        "f64".to_string(),
                                    ),
                                )?,
                            ))),
                        }
                    }
                    // left: Float, right: U128
                    Field::U128(right_v) => {
                        return match $op {
                            "/" | "%" => {
                                if right_v == 0_u128 {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Float($fct(
                                        left_v,
                                        OrderedFloat::<f64>::from_u128(right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", right_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                    )))
                                }
                            }
                            &_ => Ok(Field::Float($fct(
                                left_v,
                                OrderedFloat::<f64>::from_u128(right_v).ok_or(
                                    PipelineError::UnableToCast(
                                        format!("{}", right_v),
                                        "f64".to_string(),
                                    ),
                                )?,
                            ))),
                        }
                    }
                    // left: Float, right: Float
                    Field::Float(right_v) => {
                        return match $op {
                            "/" | "%" => {
                                if right_v == 0_f64 {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Float($fct(left_v, right_v)))
                                }
                            }
                            &_ => Ok(Field::Float($fct(left_v, right_v))),
                        }
                    }
                    // left: Float, right: Decimal
                    Field::Decimal(right_v) => {
                        return match $op {
                            "/" => Ok(Field::Decimal(
                                Decimal::from_f64(*left_v)
                                    .ok_or(PipelineError::UnableToCast(
                                        format!("{}", left_v),
                                        "Decimal".to_string(),
                                    ))?
                                    .checked_div(right_v)
                                    .ok_or(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))?,
                            )),
                            "%" => Ok(Field::Decimal(
                                Decimal::from_f64(*left_v)
                                    .ok_or(PipelineError::UnableToCast(
                                        format!("{}", left_v),
                                        "Decimal".to_string(),
                                    ))?
                                    .checked_rem(right_v)
                                    .ok_or(PipelineError::SqlError(Operation(
                                        OperationError::ModuloByZeroOrOverflow,
                                    )))?,
                            )),
                            "*" => Ok(Field::Decimal(
                                Decimal::from_f64(*left_v)
                                    .ok_or(PipelineError::UnableToCast(
                                        format!("{}", left_v),
                                        "Decimal".to_string(),
                                    ))?
                                    .checked_mul(right_v)
                                    .ok_or(PipelineError::SqlError(Operation(
                                        OperationError::MultiplicationOverflow,
                                    )))?,
                            )),
                            "+" | "-" => Ok(Field::Decimal($fct(
                                Decimal::from_f64(*left_v).ok_or(PipelineError::UnableToCast(
                                    format!("{}", left_v),
                                    "Decimal".to_string(),
                                ))?,
                                right_v,
                            ))),
                            &_ => Err(PipelineError::InvalidTypeComparison(
                                left_p,
                                right_p,
                                $op.to_string(),
                            )),
                        }
                    }
                    Field::Null => Ok(Field::Null),
                    Field::Boolean(_)
                    | Field::String(_)
                    | Field::Text(_)
                    | Field::Binary(_)
                    | Field::Timestamp(_)
                    | Field::Date(_)
                    | Field::Json(_)
                    | Field::Point(_)
                    | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::Int(left_v) => match right_p {
                    // left: Int, right: Int
                    Field::Int(right_v) => {
                        return match $op {
                            // When Int / Int division happens
                            "/" => {
                                if right_v == 0_i64 {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Float($fct(
                                        OrderedFloat::<f64>::from_i64(left_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                        OrderedFloat::<f64>::from_i64(right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", right_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                    )))
                                }
                            }
                            // When it's not division operation
                            "+" | "-" | "*" | "%" => {
                                Ok(Field::Int($fct(Wrapping(left_v), Wrapping(right_v)).0))
                            }
                            &_ => Err(PipelineError::InvalidTypeComparison(
                                left_p,
                                right_p,
                                $op.to_string(),
                            )),
                        };
                    }
                    // left: Int, right: I128
                    Field::I128(right_v) => {
                        return match $op {
                            // When Int / I128 division happens
                            "/" => {
                                if right_v == 0_i128 {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Float($fct(
                                        OrderedFloat::<f64>::from_i64(left_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                        OrderedFloat::<f64>::from_i128(right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", right_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                    )))
                                }
                            }
                            // When it's not division operation
                            "+" | "-" | "*" | "%" => Ok(Field::I128(
                                $fct(Wrapping(left_v as i128), Wrapping(right_v)).0,
                            )),
                            &_ => Err(PipelineError::InvalidTypeComparison(
                                left_p,
                                right_p,
                                $op.to_string(),
                            )),
                        };
                    }
                    // left: Int, right: UInt
                    Field::UInt(right_v) => {
                        return match $op {
                            // When Int / UInt division happens
                            "/" => {
                                if right_v == 0_u64 {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Float($fct(
                                        OrderedFloat::<f64>::from_i64(left_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                        OrderedFloat::<f64>::from_u64(right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", right_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                    )))
                                }
                            }
                            // When it's not division operation
                            "+" | "-" | "*" | "%" => Ok(Field::Int(
                                $fct(Wrapping(left_v), Wrapping(right_v as i64)).0,
                            )),
                            &_ => Err(PipelineError::InvalidTypeComparison(
                                left_p,
                                right_p,
                                $op.to_string(),
                            )),
                        };
                    }
                    // left: Int, right: U128
                    Field::U128(right_v) => {
                        return match $op {
                            // When Int / U128 division happens
                            "/" => {
                                if right_v == 0_u128 {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Float($fct(
                                        OrderedFloat::<f64>::from_i64(left_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                        OrderedFloat::<f64>::from_u128(right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", right_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                    )))
                                }
                            }
                            // When it's not division operation
                            "+" | "-" | "*" | "%" => Ok(Field::I128(
                                $fct(Wrapping(left_v as i128), Wrapping(right_v as i128)).0,
                            )),
                            &_ => Err(PipelineError::InvalidTypeComparison(
                                left_p,
                                right_p,
                                $op.to_string(),
                            )),
                        };
                    }
                    // left: Int, right: Float
                    Field::Float(right_v) => {
                        return match $op {
                            "/" | "%" => {
                                if right_v == 0_f64 {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Float($fct(
                                        OrderedFloat::<f64>::from_i64(left_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                        right_v,
                                    )))
                                }
                            }
                            &_ => Ok(Field::Float($fct(
                                OrderedFloat::<f64>::from_i64(left_v).ok_or(
                                    PipelineError::UnableToCast(
                                        format!("{}", left_v),
                                        "f64".to_string(),
                                    ),
                                )?,
                                right_v,
                            ))),
                        }
                    }

                    // left: Int, right: Decimal
                    Field::Decimal(right_v) => {
                        return match $op {
                            "/" => Ok(Field::Decimal(
                                Decimal::from_i64(left_v)
                                    .ok_or(PipelineError::UnableToCast(
                                        format!("{}", left_v),
                                        "Decimal".to_string(),
                                    ))?
                                    .checked_div(right_v)
                                    .ok_or(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))?,
                            )),
                            "%" => Ok(Field::Decimal(
                                Decimal::from_i64(left_v)
                                    .ok_or(PipelineError::UnableToCast(
                                        format!("{}", left_v),
                                        "Decimal".to_string(),
                                    ))?
                                    .checked_rem(right_v)
                                    .ok_or(PipelineError::SqlError(Operation(
                                        OperationError::ModuloByZeroOrOverflow,
                                    )))?,
                            )),
                            "*" => Ok(Field::Decimal(
                                Decimal::from_i64(left_v)
                                    .ok_or(PipelineError::UnableToCast(
                                        format!("{}", left_v),
                                        "Decimal".to_string(),
                                    ))?
                                    .checked_mul(right_v)
                                    .ok_or(PipelineError::SqlError(Operation(
                                        OperationError::MultiplicationOverflow,
                                    )))?,
                            )),
                            "+" | "-" => Ok(Field::Decimal($fct(
                                Decimal::from_i64(left_v).ok_or(PipelineError::UnableToCast(
                                    format!("{}", left_v),
                                    "Decimal".to_string(),
                                ))?,
                                right_v,
                            ))),
                            &_ => Err(PipelineError::InvalidTypeComparison(
                                left_p,
                                right_p,
                                $op.to_string(),
                            )),
                        }
                    }
                    // left: Int, right: Null
                    Field::Null => Ok(Field::Null),
                    Field::Boolean(_)
                    | Field::String(_)
                    | Field::Text(_)
                    | Field::Binary(_)
                    | Field::Timestamp(_)
                    | Field::Date(_)
                    | Field::Json(_)
                    | Field::Point(_)
                    | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::I128(left_v) => match right_p {
                    // left: I128, right: Int
                    Field::Int(right_v) => {
                        return match $op {
                            // When I128 / Int division happens
                            "/" => {
                                if right_v == 0_i64 {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Float($fct(
                                        OrderedFloat::<f64>::from_i128(left_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                        OrderedFloat::<f64>::from_i64(right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", right_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                    )))
                                }
                            }
                            // When it's not division operation
                            "+" | "-" | "*" | "%" => Ok(Field::I128(
                                $fct(Wrapping(left_v), Wrapping(right_v as i128)).0,
                            )),
                            &_ => Err(PipelineError::InvalidTypeComparison(
                                left_p,
                                right_p,
                                $op.to_string(),
                            )),
                        };
                    }
                    // left: I128, right: I128
                    Field::I128(right_v) => {
                        return match $op {
                            // When I128 / I128 division happens
                            "/" => {
                                if right_v == 0_i128 {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Float($fct(
                                        OrderedFloat::<f64>::from_i128(left_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                        OrderedFloat::<f64>::from_i128(right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", right_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                    )))
                                }
                            }
                            // When it's not division operation
                            "+" | "-" | "*" | "%" => {
                                Ok(Field::I128($fct(Wrapping(left_v), Wrapping(right_v)).0))
                            }
                            &_ => Err(PipelineError::InvalidTypeComparison(
                                left_p,
                                right_p,
                                $op.to_string(),
                            )),
                        };
                    }
                    // left: I128, right: UInt
                    Field::UInt(right_v) => {
                        return match $op {
                            // When I128 / UInt division happens
                            "/" => {
                                if right_v == 0_u64 {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Float($fct(
                                        OrderedFloat::<f64>::from_i128(left_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                        OrderedFloat::<f64>::from_u64(right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", right_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                    )))
                                }
                            }
                            // When it's not division operation
                            "+" | "-" | "*" | "%" => Ok(Field::I128(
                                $fct(Wrapping(left_v), Wrapping(right_v as i128)).0,
                            )),
                            &_ => Err(PipelineError::InvalidTypeComparison(
                                left_p,
                                right_p,
                                $op.to_string(),
                            )),
                        };
                    }
                    // left: I128, right: U128
                    Field::U128(right_v) => {
                        return match $op {
                            // When Int / U128 division happens
                            "/" => {
                                if right_v == 0_u128 {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Float($fct(
                                        OrderedFloat::<f64>::from_i128(left_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                        OrderedFloat::<f64>::from_u128(right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", right_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                    )))
                                }
                            }
                            // When it's not division operation
                            "+" | "-" | "*" | "%" => Ok(Field::I128(
                                $fct(Wrapping(left_v), Wrapping(right_v as i128)).0,
                            )),
                            &_ => Err(PipelineError::InvalidTypeComparison(
                                left_p,
                                right_p,
                                $op.to_string(),
                            )),
                        };
                    }
                    // left: I128, right: Float
                    Field::Float(right_v) => {
                        return match $op {
                            "/" | "%" => {
                                if right_v == 0_f64 {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Float($fct(
                                        OrderedFloat::<f64>::from_i128(left_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                        right_v,
                                    )))
                                }
                            }
                            &_ => Ok(Field::Float($fct(
                                OrderedFloat::<f64>::from_i128(left_v).ok_or(
                                    PipelineError::UnableToCast(
                                        format!("{}", left_v),
                                        "f64".to_string(),
                                    ),
                                )?,
                                right_v,
                            ))),
                        }
                    }
                    // left: I128, right: Decimal
                    Field::Decimal(right_v) => {
                        return match $op {
                            "/" | "%" => {
                                if right_v == dozer_types::rust_decimal::Decimal::zero() {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Decimal($fct(
                                        Decimal::from_i128(left_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "Decimal".to_string(),
                                            ),
                                        )?,
                                        right_v,
                                    )))
                                }
                            }
                            &_ => Ok(Field::Decimal($fct(
                                Decimal::from_i128(left_v).ok_or(PipelineError::UnableToCast(
                                    format!("{}", left_v),
                                    "Decimal".to_string(),
                                ))?,
                                right_v,
                            ))),
                        }
                    }
                    // left: I128, right: Null
                    Field::Null => Ok(Field::Null),
                    Field::Boolean(_)
                    | Field::String(_)
                    | Field::Text(_)
                    | Field::Binary(_)
                    | Field::Timestamp(_)
                    | Field::Date(_)
                    | Field::Json(_)
                    | Field::Point(_)
                    | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::UInt(left_v) => match right_p {
                    // left: UInt, right: Int
                    Field::Int(right_v) => {
                        return match $op {
                            // When UInt / Int division happens
                            "/" => {
                                if right_v == 0_i64 {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Float(OrderedFloat($fct(
                                        f64::from_u64(left_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                        f64::from_i64(right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", right_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                    ))))
                                }
                            }
                            // When it's not division operation
                            "+" | "-" | "*" | "%" => Ok(Field::Int(
                                $fct(Wrapping(left_v as i64), Wrapping(right_v)).0,
                            )),
                            &_ => Err(PipelineError::InvalidTypeComparison(
                                left_p,
                                right_p,
                                $op.to_string(),
                            )),
                        };
                    }
                    // left: UInt, right: I128
                    Field::I128(right_v) => {
                        return match $op {
                            // When UInt / I128 division happens
                            "/" => {
                                if right_v == 0_i128 {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Float(OrderedFloat($fct(
                                        f64::from_u64(left_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                        f64::from_i128(right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", right_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                    ))))
                                }
                            }
                            // When it's not division operation
                            "+" | "-" | "*" | "%" => Ok(Field::I128(
                                $fct(Wrapping(left_v as i128), Wrapping(right_v)).0,
                            )),
                            &_ => Err(PipelineError::InvalidTypeComparison(
                                left_p,
                                right_p,
                                $op.to_string(),
                            )),
                        };
                    }
                    // left: UInt, right: UInt
                    Field::UInt(right_v) => {
                        return match $op {
                            // When UInt / UInt division happens
                            "/" => {
                                if right_v == 0_u64 {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Float(
                                        OrderedFloat::<f64>::from_f64($fct(
                                            f64::from_u64(left_v).ok_or(
                                                PipelineError::UnableToCast(
                                                    format!("{}", left_v),
                                                    "f64".to_string(),
                                                ),
                                            )?,
                                            f64::from_u64(right_v).ok_or(
                                                PipelineError::UnableToCast(
                                                    format!("{}", right_v),
                                                    "f64".to_string(),
                                                ),
                                            )?,
                                        ))
                                        .ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "OrderedFloat".to_string(),
                                            ),
                                        )?,
                                    ))
                                }
                            }
                            // When it's not division operation
                            "+" | "-" | "*" | "%" => {
                                Ok(Field::UInt($fct(Wrapping(left_v), Wrapping(right_v)).0))
                            }
                            &_ => Err(PipelineError::InvalidTypeComparison(
                                left_p,
                                right_p,
                                $op.to_string(),
                            )),
                        };
                    }
                    // left: UInt, right: U128
                    Field::U128(right_v) => {
                        return match $op {
                            // When UInt / UInt division happens
                            "/" => {
                                if right_v == 0_u128 {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Float(
                                        OrderedFloat::<f64>::from_f64($fct(
                                            f64::from_u64(left_v).ok_or(
                                                PipelineError::UnableToCast(
                                                    format!("{}", left_v),
                                                    "f64".to_string(),
                                                ),
                                            )?,
                                            f64::from_u128(right_v).ok_or(
                                                PipelineError::UnableToCast(
                                                    format!("{}", right_v),
                                                    "f64".to_string(),
                                                ),
                                            )?,
                                        ))
                                        .ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "OrderedFloat".to_string(),
                                            ),
                                        )?,
                                    ))
                                }
                            }
                            // When it's not division operation
                            "+" | "-" | "*" | "%" => Ok(Field::U128(
                                $fct(Wrapping(left_v as u128), Wrapping(right_v)).0,
                            )),
                            &_ => Err(PipelineError::InvalidTypeComparison(
                                left_p,
                                right_p,
                                $op.to_string(),
                            )),
                        };
                    }
                    // left: UInt, right: Float
                    Field::Float(right_v) => {
                        return match $op {
                            "/" | "%" => {
                                if right_v == 0_f64 {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Float($fct(
                                        OrderedFloat::<f64>::from_u64(left_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                        right_v,
                                    )))
                                }
                            }
                            &_ => Ok(Field::Float($fct(
                                OrderedFloat::<f64>::from_u64(left_v).ok_or(
                                    PipelineError::UnableToCast(
                                        format!("{}", left_v),
                                        "f64".to_string(),
                                    ),
                                )?,
                                right_v,
                            ))),
                        }
                    }
                    // left: UInt, right: Decimal
                    Field::Decimal(right_v) => {
                        return match $op {
                            "/" => {
                                if right_v == dozer_types::rust_decimal::Decimal::zero() {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Decimal(
                                        Decimal::from_u64(left_v)
                                            .ok_or(PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "Decimal".to_string(),
                                            ))?
                                            .checked_div(right_v)
                                            .ok_or(PipelineError::SqlError(Operation(
                                                OperationError::DivisionByZeroOrOverflow,
                                            )))?,
                                    ))
                                }
                            }
                            "%" => Ok(Field::Decimal(
                                Decimal::from_u64(left_v)
                                    .ok_or(PipelineError::UnableToCast(
                                        format!("{}", left_v),
                                        "Decimal".to_string(),
                                    ))?
                                    .checked_rem(right_v)
                                    .ok_or(PipelineError::SqlError(Operation(
                                        OperationError::ModuloByZeroOrOverflow,
                                    )))?,
                            )),
                            "*" => Ok(Field::Decimal(
                                Decimal::from_u64(left_v)
                                    .ok_or(PipelineError::UnableToCast(
                                        format!("{}", left_v),
                                        "Decimal".to_string(),
                                    ))?
                                    .checked_mul(right_v)
                                    .ok_or(PipelineError::SqlError(Operation(
                                        OperationError::MultiplicationOverflow,
                                    )))?,
                            )),
                            "+" | "-" => Ok(Field::Decimal($fct(
                                Decimal::from_u64(left_v).ok_or(PipelineError::UnableToCast(
                                    format!("{}", left_v),
                                    "Decimal".to_string(),
                                ))?,
                                right_v,
                            ))),
                            &_ => Err(PipelineError::InvalidTypeComparison(
                                left_p,
                                right_p,
                                $op.to_string(),
                            )),
                        }
                    }
                    // left: UInt, right: Null
                    Field::Null => Ok(Field::Null),
                    Field::Boolean(_)
                    | Field::String(_)
                    | Field::Text(_)
                    | Field::Binary(_)
                    | Field::Timestamp(_)
                    | Field::Date(_)
                    | Field::Json(_)
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
                        return match $op {
                            // When U128 / Int division happens
                            "/" => {
                                if right_v == 0_i64 {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Float(OrderedFloat($fct(
                                        f64::from_u128(left_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                        f64::from_i64(right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", right_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                    ))))
                                }
                            }
                            // When it's not division operation
                            "+" | "-" | "*" | "%" => Ok(Field::I128(
                                $fct(Wrapping(left_v as i128), Wrapping(right_v as i128)).0,
                            )),
                            &_ => Err(PipelineError::InvalidTypeComparison(
                                left_p,
                                right_p,
                                $op.to_string(),
                            )),
                        };
                    }
                    // left: U128, right: I128
                    Field::I128(right_v) => {
                        return match $op {
                            // When U128 / I128 division happens
                            "/" => {
                                if right_v == 0_i128 {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Float(OrderedFloat($fct(
                                        f64::from_u128(left_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                        f64::from_i128(right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", right_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                    ))))
                                }
                            }
                            // When it's not division operation
                            "+" | "-" | "*" | "%" => Ok(Field::I128(
                                $fct(Wrapping(left_v as i128), Wrapping(right_v)).0,
                            )),
                            &_ => Err(PipelineError::InvalidTypeComparison(
                                left_p,
                                right_p,
                                $op.to_string(),
                            )),
                        };
                    }
                    // left: U128, right: UInt
                    Field::UInt(right_v) => {
                        return match $op {
                            // When U128 / UInt division happens
                            "/" => {
                                if right_v == 0_u64 {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Float(
                                        OrderedFloat::<f64>::from_f64($fct(
                                            f64::from_u128(left_v).ok_or(
                                                PipelineError::UnableToCast(
                                                    format!("{}", left_v),
                                                    "f64".to_string(),
                                                ),
                                            )?,
                                            f64::from_u64(right_v).ok_or(
                                                PipelineError::UnableToCast(
                                                    format!("{}", right_v),
                                                    "f64".to_string(),
                                                ),
                                            )?,
                                        ))
                                        .ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "OrderedFloat".to_string(),
                                            ),
                                        )?,
                                    ))
                                }
                            }
                            // When it's not division operation
                            "+" | "-" | "*" | "%" => Ok(Field::U128(
                                $fct(Wrapping(left_v), Wrapping(right_v as u128)).0,
                            )),
                            &_ => Err(PipelineError::InvalidTypeComparison(
                                left_p,
                                right_p,
                                $op.to_string(),
                            )),
                        };
                    }
                    // left: U128, right: U128
                    Field::U128(right_v) => {
                        return match $op {
                            // When U128 / U128 division happens
                            "/" => {
                                if right_v == 0_u128 {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Float(
                                        OrderedFloat::<f64>::from_f64($fct(
                                            f64::from_u128(left_v).ok_or(
                                                PipelineError::UnableToCast(
                                                    format!("{}", left_v),
                                                    "f64".to_string(),
                                                ),
                                            )?,
                                            f64::from_u128(right_v).ok_or(
                                                PipelineError::UnableToCast(
                                                    format!("{}", right_v),
                                                    "f64".to_string(),
                                                ),
                                            )?,
                                        ))
                                        .ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "OrderedFloat".to_string(),
                                            ),
                                        )?,
                                    ))
                                }
                            }
                            // When it's not division operation
                            "+" | "-" | "*" | "%" => {
                                Ok(Field::U128($fct(Wrapping(left_v), Wrapping(right_v)).0))
                            }
                            &_ => Err(PipelineError::InvalidTypeComparison(
                                left_p,
                                right_p,
                                $op.to_string(),
                            )),
                        };
                    }
                    // left: U128, right: Float
                    Field::Float(right_v) => {
                        return match $op {
                            "/" | "%" => {
                                if right_v == 0_f64 {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Float($fct(
                                        OrderedFloat::<f64>::from_u128(left_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "f64".to_string(),
                                            ),
                                        )?,
                                        right_v,
                                    )))
                                }
                            }
                            &_ => Ok(Field::Float($fct(
                                OrderedFloat::<f64>::from_u128(left_v).ok_or(
                                    PipelineError::UnableToCast(
                                        format!("{}", left_v),
                                        "f64".to_string(),
                                    ),
                                )?,
                                right_v,
                            ))),
                        }
                    }
                    // left: U128, right: Decimal
                    Field::Decimal(right_v) => {
                        return match $op {
                            "/" | "%" => {
                                if right_v == dozer_types::rust_decimal::Decimal::zero() {
                                    Err(PipelineError::SqlError(Operation(
                                        OperationError::DivisionByZeroOrOverflow,
                                    )))
                                } else {
                                    Ok(Field::Decimal($fct(
                                        Decimal::from_u128(left_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "Decimal".to_string(),
                                            ),
                                        )?,
                                        right_v,
                                    )))
                                }
                            }
                            &_ => Ok(Field::Decimal($fct(
                                Decimal::from_u128(left_v).ok_or(PipelineError::UnableToCast(
                                    format!("{}", left_v),
                                    "Decimal".to_string(),
                                ))?,
                                right_v,
                            ))),
                        }
                    }
                    // left: U128, right: Null
                    Field::Null => Ok(Field::Null),
                    Field::Boolean(_)
                    | Field::String(_)
                    | Field::Text(_)
                    | Field::Binary(_)
                    | Field::Timestamp(_)
                    | Field::Date(_)
                    | Field::Json(_)
                    | Field::Point(_)
                    | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::Decimal(left_v) => {
                    return match $op {
                        "/" => {
                            match right_p {
                                // left: Decimal, right: Int
                                Field::Int(right_v) => {
                                    if right_v == 0_i64 {
                                        Err(PipelineError::SqlError(Operation(
                                            OperationError::DivisionByZeroOrOverflow,
                                        )))
                                    } else {
                                        Ok(Field::Decimal(
                                            left_v
                                                .checked_div(Decimal::from_i64(right_v).ok_or(
                                                    PipelineError::UnableToCast(
                                                        format!("{}", left_v),
                                                        "Decimal".to_string(),
                                                    ),
                                                )?)
                                                .ok_or(PipelineError::SqlError(Operation(
                                                    OperationError::DivisionByZeroOrOverflow,
                                                )))?,
                                        ))
                                    }
                                }
                                // left: Decimal, right: I128
                                Field::I128(right_v) => {
                                    if right_v == 0_i128 {
                                        Err(PipelineError::SqlError(Operation(
                                            OperationError::DivisionByZeroOrOverflow,
                                        )))
                                    } else {
                                        Ok(Field::Decimal(
                                            left_v
                                                .checked_div(Decimal::from_i128(right_v).ok_or(
                                                    PipelineError::UnableToCast(
                                                        format!("{}", left_v),
                                                        "Decimal".to_string(),
                                                    ),
                                                )?)
                                                .ok_or(PipelineError::SqlError(Operation(
                                                    OperationError::DivisionByZeroOrOverflow,
                                                )))?,
                                        ))
                                    }
                                }
                                // left: Decimal, right: UInt
                                Field::UInt(right_v) => {
                                    if right_v == 0_u64 {
                                        Err(PipelineError::SqlError(Operation(
                                            OperationError::DivisionByZeroOrOverflow,
                                        )))
                                    } else {
                                        Ok(Field::Decimal(
                                            left_v
                                                .checked_div(Decimal::from_u64(right_v).ok_or(
                                                    PipelineError::UnableToCast(
                                                        format!("{}", right_v),
                                                        "Decimal".to_string(),
                                                    ),
                                                )?)
                                                .ok_or(PipelineError::SqlError(Operation(
                                                    OperationError::DivisionByZeroOrOverflow,
                                                )))?,
                                        ))
                                    }
                                }
                                // left: Decimal, right: U128
                                Field::U128(right_v) => {
                                    if right_v == 0_u128 {
                                        Err(PipelineError::SqlError(Operation(
                                            OperationError::DivisionByZeroOrOverflow,
                                        )))
                                    } else {
                                        Ok(Field::Decimal(
                                            left_v
                                                .checked_div(Decimal::from_u128(right_v).ok_or(
                                                    PipelineError::UnableToCast(
                                                        format!("{}", right_v),
                                                        "Decimal".to_string(),
                                                    ),
                                                )?)
                                                .ok_or(PipelineError::SqlError(Operation(
                                                    OperationError::DivisionByZeroOrOverflow,
                                                )))?,
                                        ))
                                    }
                                }
                                // left: Decimal, right: Float
                                Field::Float(right_v) => {
                                    if right_v == 0_f64 {
                                        Err(PipelineError::SqlError(Operation(
                                            OperationError::DivisionByZeroOrOverflow,
                                        )))
                                    } else {
                                        Ok(Field::Decimal(
                                            left_v
                                                .checked_div(Decimal::from_f64(*right_v).ok_or(
                                                    PipelineError::UnableToCast(
                                                        format!("{}", right_v),
                                                        "Decimal".to_string(),
                                                    ),
                                                )?)
                                                .ok_or(PipelineError::SqlError(Operation(
                                                    OperationError::DivisionByZeroOrOverflow,
                                                )))?,
                                        ))
                                    }
                                }
                                // left: Decimal, right: Null
                                Field::Null => Ok(Field::Null),
                                // left: Decimal, right: Decimal
                                Field::Decimal(right_v) => Ok(Field::Decimal(
                                    left_v.checked_div(right_v).ok_or(PipelineError::SqlError(
                                        Operation(OperationError::DivisionByZeroOrOverflow),
                                    ))?,
                                )),
                                Field::Boolean(_)
                                | Field::String(_)
                                | Field::Text(_)
                                | Field::Binary(_)
                                | Field::Timestamp(_)
                                | Field::Date(_)
                                | Field::Json(_)
                                | Field::Point(_)
                                | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                                    left_p,
                                    right_p,
                                    $op.to_string(),
                                )),
                            }
                        }
                        "%" => {
                            match right_p {
                                // left: Decimal, right: Int
                                Field::Int(right_v) => Ok(Field::Decimal(
                                    left_v
                                        .checked_rem(Decimal::from_i64(right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "Decimal".to_string(),
                                            ),
                                        )?)
                                        .ok_or(PipelineError::SqlError(Operation(
                                            OperationError::ModuloByZeroOrOverflow,
                                        )))?,
                                )),
                                // left: Decimal, right: I128
                                Field::I128(right_v) => Ok(Field::Decimal(
                                    left_v
                                        .checked_rem(Decimal::from_i128(right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "Decimal".to_string(),
                                            ),
                                        )?)
                                        .ok_or(PipelineError::SqlError(Operation(
                                            OperationError::ModuloByZeroOrOverflow,
                                        )))?,
                                )),
                                // left: Decimal, right: UInt
                                Field::UInt(right_v) => Ok(Field::Decimal(
                                    left_v
                                        .checked_rem(Decimal::from_u64(right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", right_v),
                                                "Decimal".to_string(),
                                            ),
                                        )?)
                                        .ok_or(PipelineError::SqlError(Operation(
                                            OperationError::ModuloByZeroOrOverflow,
                                        )))?,
                                )),
                                // left: Decimal, right: U128
                                Field::U128(right_v) => Ok(Field::Decimal(
                                    left_v
                                        .checked_rem(Decimal::from_u128(right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", right_v),
                                                "Decimal".to_string(),
                                            ),
                                        )?)
                                        .ok_or(PipelineError::SqlError(Operation(
                                            OperationError::ModuloByZeroOrOverflow,
                                        )))?,
                                )),
                                // left: Decimal, right: Float
                                Field::Float(right_v) => Ok(Field::Decimal(
                                    left_v
                                        .checked_rem(Decimal::from_f64(*right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", right_v),
                                                "Decimal".to_string(),
                                            ),
                                        )?)
                                        .ok_or(PipelineError::SqlError(Operation(
                                            OperationError::ModuloByZeroOrOverflow,
                                        )))?,
                                )),
                                // left: Decimal, right: Null
                                Field::Null => Ok(Field::Null),
                                // left: Decimal, right: Decimal
                                Field::Decimal(right_v) => Ok(Field::Decimal(
                                    left_v.checked_rem(right_v).ok_or(PipelineError::SqlError(
                                        Operation(OperationError::ModuloByZeroOrOverflow),
                                    ))?,
                                )),
                                Field::Boolean(_)
                                | Field::String(_)
                                | Field::Text(_)
                                | Field::Binary(_)
                                | Field::Timestamp(_)
                                | Field::Date(_)
                                | Field::Json(_)
                                | Field::Point(_)
                                | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                                    left_p,
                                    right_p,
                                    $op.to_string(),
                                )),
                            }
                        }
                        "*" => {
                            match right_p {
                                // left: Decimal, right: Int
                                Field::Int(right_v) => Ok(Field::Decimal(
                                    left_v
                                        .checked_mul(Decimal::from_i64(right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "Decimal".to_string(),
                                            ),
                                        )?)
                                        .ok_or(PipelineError::SqlError(Operation(
                                            OperationError::MultiplicationOverflow,
                                        )))?,
                                )),
                                // left: Decimal, right: I128
                                Field::I128(right_v) => Ok(Field::Decimal(
                                    left_v
                                        .checked_mul(Decimal::from_i128(right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "Decimal".to_string(),
                                            ),
                                        )?)
                                        .ok_or(PipelineError::SqlError(Operation(
                                            OperationError::MultiplicationOverflow,
                                        )))?,
                                )),
                                // left: Decimal, right: UInt
                                Field::UInt(right_v) => Ok(Field::Decimal(
                                    left_v
                                        .checked_mul(Decimal::from_u64(right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", right_v),
                                                "Decimal".to_string(),
                                            ),
                                        )?)
                                        .ok_or(PipelineError::SqlError(Operation(
                                            OperationError::MultiplicationOverflow,
                                        )))?,
                                )),
                                // left: Decimal, right: U128
                                Field::U128(right_v) => Ok(Field::Decimal(
                                    left_v
                                        .checked_mul(Decimal::from_u128(right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", right_v),
                                                "Decimal".to_string(),
                                            ),
                                        )?)
                                        .ok_or(PipelineError::SqlError(Operation(
                                            OperationError::MultiplicationOverflow,
                                        )))?,
                                )),
                                // left: Decimal, right: Float
                                Field::Float(right_v) => Ok(Field::Decimal(
                                    left_v
                                        .checked_mul(Decimal::from_f64(*right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", right_v),
                                                "Decimal".to_string(),
                                            ),
                                        )?)
                                        .ok_or(PipelineError::SqlError(Operation(
                                            OperationError::MultiplicationOverflow,
                                        )))?,
                                )),
                                // left: Decimal, right: Null
                                Field::Null => Ok(Field::Null),
                                // left: Decimal, right: Decimal
                                Field::Decimal(right_v) => Ok(Field::Decimal(
                                    left_v.checked_mul(right_v).ok_or(PipelineError::SqlError(
                                        Operation(OperationError::MultiplicationOverflow),
                                    ))?,
                                )),
                                Field::Boolean(_)
                                | Field::String(_)
                                | Field::Text(_)
                                | Field::Binary(_)
                                | Field::Timestamp(_)
                                | Field::Date(_)
                                | Field::Json(_)
                                | Field::Point(_)
                                | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                                    left_p,
                                    right_p,
                                    $op.to_string(),
                                )),
                            }
                        }
                        "+" | "-" => {
                            match right_p {
                                // left: Decimal, right: Int
                                Field::Int(right_v) => Ok(Field::Decimal($fct(
                                    left_v,
                                    Decimal::from_i64(right_v).ok_or(
                                        PipelineError::UnableToCast(
                                            format!("{}", left_v),
                                            "Decimal".to_string(),
                                        ),
                                    )?,
                                ))),
                                // left: Decimal, right: I128
                                Field::I128(right_v) => Ok(Field::Decimal($fct(
                                    left_v,
                                    Decimal::from_i128(right_v).ok_or(
                                        PipelineError::UnableToCast(
                                            format!("{}", left_v),
                                            "Decimal".to_string(),
                                        ),
                                    )?,
                                ))),
                                // left: Decimal, right: UInt
                                Field::UInt(right_v) => Ok(Field::Decimal($fct(
                                    left_v,
                                    Decimal::from_u64(right_v).ok_or(
                                        PipelineError::UnableToCast(
                                            format!("{}", right_v),
                                            "Decimal".to_string(),
                                        ),
                                    )?,
                                ))),
                                // left: Decimal, right: U128
                                Field::U128(right_v) => Ok(Field::Decimal($fct(
                                    left_v,
                                    Decimal::from_u128(right_v).ok_or(
                                        PipelineError::UnableToCast(
                                            format!("{}", right_v),
                                            "Decimal".to_string(),
                                        ),
                                    )?,
                                ))),
                                // left: Decimal, right: Float
                                Field::Float(right_v) => Ok(Field::Decimal($fct(
                                    left_v,
                                    Decimal::from_f64(*right_v).ok_or(
                                        PipelineError::UnableToCast(
                                            format!("{}", right_v),
                                            "Decimal".to_string(),
                                        ),
                                    )?,
                                ))),
                                // left: Decimal, right: Null
                                Field::Null => Ok(Field::Null),
                                // left: Decimal, right: Decimal
                                Field::Decimal(right_v) => {
                                    Ok(Field::Decimal($fct(left_v, right_v)))
                                }
                                Field::Boolean(_)
                                | Field::String(_)
                                | Field::Text(_)
                                | Field::Binary(_)
                                | Field::Timestamp(_)
                                | Field::Date(_)
                                | Field::Json(_)
                                | Field::Point(_)
                                | Field::Duration(_) => Err(PipelineError::InvalidTypeComparison(
                                    left_p,
                                    right_p,
                                    $op.to_string(),
                                )),
                            }
                        }
                        &_ => Err(PipelineError::InvalidTypeComparison(
                            left_p,
                            right_p,
                            $op.to_string(),
                        )),
                    };
                }
                // right: Null, right: *
                Field::Null => Ok(Field::Null),
                Field::Boolean(_)
                | Field::String(_)
                | Field::Text(_)
                | Field::Binary(_)
                | Field::Date(_)
                | Field::Json(_)
                | Field::Point(_) => Err(PipelineError::InvalidTypeComparison(
                    left_p,
                    right_p,
                    $op.to_string(),
                )),
            }
        }
    };
}

define_math_operator!(evaluate_add, "+", |a, b| { a + b }, 0);
define_math_operator!(evaluate_sub, "-", |a, b| { a - b }, 0);
define_math_operator!(evaluate_mul, "*", |a, b| { a * b }, 0);
define_math_operator!(evaluate_div, "/", |a, b| { a / b }, 1);
define_math_operator!(evaluate_mod, "%", |a, b| { a % b }, 0);

pub fn evaluate_plus(
    schema: &Schema,
    expression: &Expression,
    record: &ProcessorRecord,
) -> Result<Field, PipelineError> {
    let expression_result = expression.evaluate(record, schema)?;
    match expression_result {
        Field::UInt(v) => Ok(Field::UInt(v)),
        Field::U128(v) => Ok(Field::U128(v)),
        Field::Int(v) => Ok(Field::Int(v)),
        Field::I128(v) => Ok(Field::I128(v)),
        Field::Float(v) => Ok(Field::Float(v)),
        Field::Decimal(v) => Ok(Field::Decimal(v)),
        Field::Boolean(_)
        | Field::String(_)
        | Field::Text(_)
        | Field::Binary(_)
        | Field::Timestamp(_)
        | Field::Date(_)
        | Field::Json(_)
        | Field::Point(_)
        | Field::Duration(_)
        | Field::Null => Err(PipelineError::InvalidType(
            expression_result,
            "+".to_string(),
        )),
    }
}

pub fn evaluate_minus(
    schema: &Schema,
    expression: &Expression,
    record: &ProcessorRecord,
) -> Result<Field, PipelineError> {
    let expression_result = expression.evaluate(record, schema)?;
    match expression_result {
        Field::UInt(v) => Ok(Field::UInt(v)),
        Field::U128(v) => Ok(Field::U128(v)),
        Field::Int(v) => Ok(Field::Int(-v)),
        Field::I128(v) => Ok(Field::I128(-v)),
        Field::Float(v) => Ok(Field::Float(-v)),
        Field::Decimal(v) => Ok(Field::Decimal(v.neg())),
        Field::Timestamp(dt) => Ok(Field::Timestamp(dt.checked_add_signed(chrono::Duration::nanoseconds(1)).unwrap())),
        Field::Boolean(_)
        | Field::String(_)
        | Field::Text(_)
        | Field::Binary(_)
        // | Field::Timestamp(_)
        | Field::Date(_)
        | Field::Json(_)
        | Field::Point(_)
        | Field::Duration(_)
        | Field::Null => Err(PipelineError::InvalidType(
            expression_result,
            "-".to_string(),
        )),
    }
}
