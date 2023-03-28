use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::SqlError::OperationError;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use dozer_types::rust_decimal::Decimal;
use dozer_types::types::Schema;
use dozer_types::{
    ordered_float::OrderedFloat,
    types::{Field, Record},
};
use num_traits::FromPrimitive;

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
                Field::Timestamp(left_v) => match right_p {
                    Field::Timestamp(right_v) => match $op {
                        "-" => {
                            let duration = left_v - right_v;
                            duration
                                .num_nanoseconds()
                                .ok_or(PipelineError::UnableToCast(
                                    format!("{}", duration),
                                    "i64".to_string(),
                                ))
                                .map(Field::Int)
                        }
                        _ => Err(PipelineError::InvalidTypeComparison(
                            left_p,
                            right_p,
                            $op.to_string(),
                        )),
                    },
                    _ => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::Float(left_v) => match right_p {
                    // left: Float, right: Int
                    Field::Int(right_v) => Ok(Field::Float($fct(
                        left_v,
                        OrderedFloat::<f64>::from_i64(right_v).ok_or(
                            PipelineError::UnableToCast(format!("{}", right_v), "f64".to_string()),
                        )?,
                    ))),
                    // left: Float, right: UInt
                    Field::UInt(right_v) => Ok(Field::Float($fct(
                        left_v,
                        OrderedFloat::<f64>::from_u64(right_v).ok_or(
                            PipelineError::UnableToCast(format!("{}", right_v), "f64".to_string()),
                        )?,
                    ))),
                    // left: Float, right: Float
                    Field::Float(right_v) => Ok(Field::Float($fct(left_v, right_v))),
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
                                    .ok_or(PipelineError::SqlError(OperationError(
                                        $op.to_string(),
                                        "Division Error".to_string(),
                                    )))?,
                            )),
                            "%" => Ok(Field::Decimal(
                                Decimal::from_f64(*left_v)
                                    .ok_or(PipelineError::UnableToCast(
                                        format!("{}", left_v),
                                        "Decimal".to_string(),
                                    ))?
                                    .checked_rem(right_v)
                                    .ok_or(PipelineError::SqlError(OperationError(
                                        $op.to_string(),
                                        "Modulo Error".to_string(),
                                    )))?,
                            )),
                            "+" | "-" | "*" => Ok(Field::Decimal($fct(
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
                    _ => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::Int(left_v) => match right_p {
                    // left: Int, right: Int
                    Field::Int(right_v) => {
                        return match ($t) {
                            // When Int / Int division happens
                            1 => Ok(Field::Float($fct(
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
                            ))),
                            // When it's not division operation
                            _ => Ok(Field::Int($fct(Wrapping(left_v), Wrapping(right_v)).0)),
                        };
                    }
                    // left: Int, right: UInt
                    Field::UInt(right_v) => {
                        return match ($t) {
                            // When Int / UInt division happens
                            1 => Ok(Field::Float($fct(
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
                            ))),
                            // When it's not division operation
                            _ => Ok(Field::Int(
                                $fct(Wrapping(left_v), Wrapping(right_v as i64)).0,
                            )),
                        };
                    }
                    // left: Int, right: Float
                    Field::Float(right_v) => Ok(Field::Float($fct(
                        OrderedFloat::<f64>::from_i64(left_v).ok_or(
                            PipelineError::UnableToCast(format!("{}", left_v), "f64".to_string()),
                        )?,
                        right_v,
                    ))),
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
                                    .ok_or(PipelineError::SqlError(OperationError(
                                        $op.to_string(),
                                        "Division Error".to_string(),
                                    )))?,
                            )),
                            "%" => Ok(Field::Decimal(
                                Decimal::from_i64(left_v)
                                    .ok_or(PipelineError::UnableToCast(
                                        format!("{}", left_v),
                                        "Decimal".to_string(),
                                    ))?
                                    .checked_rem(right_v)
                                    .ok_or(PipelineError::SqlError(OperationError(
                                        $op.to_string(),
                                        "Modulo Error".to_string(),
                                    )))?,
                            )),
                            "+" | "-" | "*" => Ok(Field::Decimal($fct(
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
                    _ => Err(PipelineError::InvalidTypeComparison(
                        left_p,
                        right_p,
                        $op.to_string(),
                    )),
                },
                Field::UInt(left_v) => match right_p {
                    // left: UInt, right: Int
                    Field::Int(right_v) => {
                        return match ($t) {
                            // When UInt / Int division happens
                            1 => Ok(Field::Float(OrderedFloat($fct(
                                f64::from_u64(left_v).ok_or(PipelineError::UnableToCast(
                                    format!("{}", left_v),
                                    "f64".to_string(),
                                ))?,
                                f64::from_i64(right_v).ok_or(PipelineError::UnableToCast(
                                    format!("{}", right_v),
                                    "f64".to_string(),
                                ))?,
                            )))),
                            // When it's not division operation
                            _ => Ok(Field::Int(
                                $fct(Wrapping(left_v as i64), Wrapping(right_v)).0,
                            )),
                        };
                    }
                    // left: UInt, right: UInt
                    Field::UInt(right_v) => {
                        return match ($t) {
                            // When UInt / UInt division happens
                            1 => Ok(Field::Float(
                                OrderedFloat::<f64>::from_f64($fct(
                                    f64::from_u64(left_v).ok_or(PipelineError::UnableToCast(
                                        format!("{}", left_v),
                                        "f64".to_string(),
                                    ))?,
                                    f64::from_u64(right_v).ok_or(PipelineError::UnableToCast(
                                        format!("{}", right_v),
                                        "f64".to_string(),
                                    ))?,
                                ))
                                .ok_or(PipelineError::UnableToCast(
                                    format!("{}", left_v),
                                    "OrderedFloat".to_string(),
                                ))?,
                            )),
                            // When it's not division operation
                            _ => Ok(Field::UInt($fct(Wrapping(left_v), Wrapping(right_v)).0)),
                        };
                    }
                    // left: UInt, right: Float
                    Field::Float(right_v) => Ok(Field::Float($fct(
                        OrderedFloat::<f64>::from_u64(left_v).ok_or(
                            PipelineError::UnableToCast(format!("{}", left_v), "f64".to_string()),
                        )?,
                        right_v,
                    ))),
                    // left: UInt, right: Decimal
                    Field::Decimal(right_v) => {
                        return match $op {
                            "/" => Ok(Field::Decimal(
                                Decimal::from_u64(left_v)
                                    .ok_or(PipelineError::UnableToCast(
                                        format!("{}", left_v),
                                        "Decimal".to_string(),
                                    ))?
                                    .checked_div(right_v)
                                    .ok_or(PipelineError::SqlError(OperationError(
                                        $op.to_string(),
                                        "Division Error".to_string(),
                                    )))?,
                            )),
                            "%" => Ok(Field::Decimal(
                                Decimal::from_u64(left_v)
                                    .ok_or(PipelineError::UnableToCast(
                                        format!("{}", left_v),
                                        "Decimal".to_string(),
                                    ))?
                                    .checked_rem(right_v)
                                    .ok_or(PipelineError::SqlError(OperationError(
                                        $op.to_string(),
                                        "Modulo Error".to_string(),
                                    )))?,
                            )),
                            "+" | "-" | "*" => Ok(Field::Decimal($fct(
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
                    _ => Err(PipelineError::InvalidTypeComparison(
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
                                Field::Int(right_v) => Ok(Field::Decimal(
                                    left_v
                                        .checked_div(Decimal::from_i64(right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", left_v),
                                                "Decimal".to_string(),
                                            ),
                                        )?)
                                        .ok_or(PipelineError::SqlError(OperationError(
                                            $op.to_string(),
                                            "Division Error".to_string(),
                                        )))?,
                                )),
                                // left: Decimal, right: UInt
                                Field::UInt(right_v) => Ok(Field::Decimal(
                                    left_v
                                        .checked_div(Decimal::from_u64(right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", right_v),
                                                "Decimal".to_string(),
                                            ),
                                        )?)
                                        .ok_or(PipelineError::SqlError(OperationError(
                                            $op.to_string(),
                                            "Division Error".to_string(),
                                        )))?,
                                )),
                                // left: Decimal, right: Float
                                Field::Float(right_v) => Ok(Field::Decimal(
                                    left_v
                                        .checked_div(Decimal::from_f64(*right_v).ok_or(
                                            PipelineError::UnableToCast(
                                                format!("{}", right_v),
                                                "Decimal".to_string(),
                                            ),
                                        )?)
                                        .ok_or(PipelineError::SqlError(OperationError(
                                            $op.to_string(),
                                            "Division Error".to_string(),
                                        )))?,
                                )),
                                // left: Decimal, right: Null
                                Field::Null => Ok(Field::Null),
                                // left: Decimal, right: Decimal
                                Field::Decimal(right_v) => {
                                    Ok(Field::Decimal(left_v.checked_div(right_v).ok_or(
                                        PipelineError::SqlError(OperationError(
                                            $op.to_string(),
                                            "Division Error".to_string(),
                                        )),
                                    )?))
                                }
                                _ => Err(PipelineError::InvalidTypeComparison(
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
                                        .ok_or(PipelineError::SqlError(OperationError(
                                            $op.to_string(),
                                            "Modulo Error".to_string(),
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
                                        .ok_or(PipelineError::SqlError(OperationError(
                                            $op.to_string(),
                                            "Modulo Error".to_string(),
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
                                        .ok_or(PipelineError::SqlError(OperationError(
                                            $op.to_string(),
                                            "Modulo Error".to_string(),
                                        )))?,
                                )),
                                // left: Decimal, right: Null
                                Field::Null => Ok(Field::Null),
                                // left: Decimal, right: Decimal
                                Field::Decimal(right_v) => Ok(Field::Decimal(
                                    left_v.checked_rem(right_v).ok_or(PipelineError::SqlError(
                                        OperationError($op.to_string(), "Modulo Error".to_string()),
                                    ))?,
                                )),
                                _ => Err(PipelineError::InvalidTypeComparison(
                                    left_p,
                                    right_p,
                                    $op.to_string(),
                                )),
                            }
                        }
                        "+" | "-" | "*" => {
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
                                _ => Err(PipelineError::InvalidTypeComparison(
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
                _ => Err(PipelineError::InvalidTypeComparison(
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
    record: &Record,
) -> Result<Field, PipelineError> {
    let expression_result = expression.evaluate(record, schema)?;
    match expression_result {
        Field::UInt(v) => Ok(Field::UInt(v)),
        Field::Int(v) => Ok(Field::Int(v)),
        Field::Float(v) => Ok(Field::Float(v)),
        Field::Decimal(v) => Ok(Field::Decimal(v)),
        not_supported_field => Err(PipelineError::InvalidType(
            not_supported_field,
            "+".to_string(),
        )),
    }
}

pub fn evaluate_minus(
    schema: &Schema,
    expression: &Expression,
    record: &Record,
) -> Result<Field, PipelineError> {
    let expression_result = expression.evaluate(record, schema)?;
    match expression_result {
        Field::UInt(v) => Ok(Field::UInt(v)),
        Field::Int(v) => Ok(Field::Int(-v)),
        Field::Float(v) => Ok(Field::Float(-v)),
        Field::Decimal(v) => Ok(Field::Decimal(v.neg())),
        not_supported_field => Err(PipelineError::InvalidType(
            not_supported_field,
            "-".to_string(),
        )),
    }
}
