use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use dozer_types::chrono::DateTime;
use dozer_types::types::{FieldDefinition, FieldType, Schema, SourceDefinition};
use dozer_types::{
    ordered_float::OrderedFloat,
    types::{Field, Record},
};
use num_traits::cast::*;

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
                            Ok(Field::Int(duration.num_milliseconds()))
                        }
                        _ => Err(PipelineError::InvalidOperandType($op.to_string())),
                    },
                    _ => Err(PipelineError::InvalidOperandType($op.to_string())),
                },
                Field::Float(left_v) => match right_p {
                    Field::Int(right_v) => Ok(Field::Float($fct(
                        left_v,
                        OrderedFloat::<f64>::from_i64(right_v).ok_or(
                            PipelineError::InvalidOperandType(format!(
                                "Unable to cast {} to float",
                                right_v
                            )),
                        )?,
                    ))),
                    Field::UInt(right_v) => Ok(Field::Float($fct(
                        left_v,
                        OrderedFloat::<f64>::from_u64(right_v).ok_or(
                            PipelineError::InvalidOperandType(format!(
                                "Unable to cast {} to float",
                                right_v
                            )),
                        )?,
                    ))),
                    Field::Float(right_v) => Ok(Field::Float($fct(left_v, right_v))),
                    _ => Err(PipelineError::InvalidOperandType($op.to_string())),
                },
                Field::Int(left_v) => match right_p {
                    Field::Int(right_v) => {
                        return match ($t) {
                            1 => Ok(Field::Float($fct(
                                OrderedFloat::<f64>::from_i64(left_v).ok_or(
                                    PipelineError::InvalidOperandType(format!(
                                        "Unable to cast {} to float",
                                        left_v
                                    )),
                                )?,
                                OrderedFloat::<f64>::from_i64(right_v).ok_or(
                                    PipelineError::InvalidOperandType(format!(
                                        "Unable to cast {} to float",
                                        right_v
                                    )),
                                )?,
                            ))),
                            _ => Ok(Field::Int($fct(left_v, right_v))),
                        };
                    }
                    Field::UInt(right_v) => {
                        return match ($t) {
                            1 => Ok(Field::Float($fct(
                                OrderedFloat::<f64>::from_i64(left_v).ok_or(
                                    PipelineError::InvalidOperandType(format!(
                                        "Unable to cast {} to float",
                                        left_v
                                    )),
                                )?,
                                OrderedFloat::<f64>::from_u64(right_v).ok_or(
                                    PipelineError::InvalidOperandType(format!(
                                        "Unable to cast {} to float",
                                        right_v
                                    )),
                                )?,
                            ))),
                            _ => Ok(Field::Int($fct(left_v, right_v as i64))),
                        };
                    }
                    Field::Float(right_v) => Ok(Field::Float($fct(
                        OrderedFloat::<f64>::from_i64(left_v).ok_or(
                            PipelineError::InvalidOperandType(format!(
                                "Unable to cast {} to float",
                                left_v
                            )),
                        )?,
                        right_v,
                    ))),
                    _ => Err(PipelineError::InvalidOperandType($op.to_string())),
                },
                Field::UInt(left_v) => match right_p {
                    Field::Int(right_v) => {
                        return match ($t) {
                            1 => Ok(Field::Float($fct(
                                OrderedFloat::<f64>::from_u64(left_v).ok_or(
                                    PipelineError::InvalidOperandType(format!(
                                        "Unable to cast {} to float",
                                        left_v
                                    )),
                                )?,
                                OrderedFloat::<f64>::from_i64(right_v).ok_or(
                                    PipelineError::InvalidOperandType(format!(
                                        "Unable to cast {} to float",
                                        right_v
                                    )),
                                )?,
                            ))),
                            _ => Ok(Field::Int($fct(left_v as i64, right_v))),
                        };
                    }
                    Field::UInt(right_v) => {
                        return match ($t) {
                            1 => Ok(Field::Float($fct(
                                OrderedFloat::<f64>::from_u64(left_v).ok_or(
                                    PipelineError::InvalidOperandType(format!(
                                        "Unable to cast {} to float",
                                        left_v
                                    )),
                                )?,
                                OrderedFloat::<f64>::from_u64(right_v).ok_or(
                                    PipelineError::InvalidOperandType(format!(
                                        "Unable to cast {} to float",
                                        right_v
                                    )),
                                )?,
                            ))),
                            _ => Ok(Field::UInt($fct(left_v, right_v))),
                        };
                    }
                    Field::Float(right_v) => Ok(Field::Float($fct(
                        OrderedFloat::<f64>::from_u64(left_v).ok_or(
                            PipelineError::InvalidOperandType(format!(
                                "Unable to cast {} to float",
                                left_v
                            )),
                        )?,
                        right_v,
                    ))),
                    _ => Err(PipelineError::InvalidOperandType($op.to_string())),
                },
                _ => Err(PipelineError::InvalidOperandType($op.to_string())),
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
        Field::Int(v) => Ok(Field::Int(v)),
        Field::Float(v) => Ok(Field::Float(v)),
        _ => Err(PipelineError::InvalidOperandType("+".to_string())),
    }
}

pub fn evaluate_minus(
    schema: &Schema,
    expression: &Expression,
    record: &Record,
) -> Result<Field, PipelineError> {
    let expression_result = expression.evaluate(record, schema)?;
    match expression_result {
        Field::Int(v) => Ok(Field::Int(-v)),
        Field::Float(v) => Ok(Field::Float(-v)),
        _ => Err(PipelineError::InvalidOperandType("-".to_string())),
    }
}

#[test]
fn test_timestamp_difference() {
    let schema = Schema::empty()
        .field(
            FieldDefinition::new(
                String::from("a"),
                FieldType::Timestamp,
                false,
                SourceDefinition::Dynamic,
            ),
            true,
        )
        .field(
            FieldDefinition::new(
                String::from("b"),
                FieldType::Timestamp,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .clone();

    let record = Record::new(
        None,
        vec![
            Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:13:00Z").unwrap()),
            Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:12:10Z").unwrap()),
        ],
        Some(1),
    );

    let result = evaluate_sub(
        &schema,
        &Expression::Column { index: 0 },
        &Expression::Column { index: 1 },
        &record,
    )
    .unwrap();
    assert_eq!(result, Field::Int(50000));

    let result = evaluate_sub(
        &schema,
        &Expression::Column { index: 1 },
        &Expression::Column { index: 0 },
        &record,
    )
    .unwrap();
    assert_eq!(result, Field::Int(-50000));
}
