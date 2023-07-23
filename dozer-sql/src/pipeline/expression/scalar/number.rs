use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::InvalidFunctionArgument;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use crate::pipeline::expression::scalar::common::ScalarFunctionType;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, FieldType, ProcessorRecord, Schema};
use num_traits::{Float, ToPrimitive};

pub(crate) fn evaluate_abs(
    schema: &Schema,
    arg: &Expression,
    record: &ProcessorRecord,
) -> Result<Field, PipelineError> {
    let value = arg.evaluate(record, schema)?;
    match value {
        Field::UInt(u) => Ok(Field::UInt(u)),
        Field::U128(u) => Ok(Field::U128(u)),
        Field::Int(i) => Ok(Field::Int(i.abs())),
        Field::I128(i) => Ok(Field::I128(i.abs())),
        Field::Float(f) => Ok(Field::Float(f.abs())),
        Field::Decimal(d) => Ok(Field::Decimal(d.abs())),
        Field::Boolean(_)
        | Field::String(_)
        | Field::Text(_)
        | Field::Date(_)
        | Field::Timestamp(_)
        | Field::Binary(_)
        | Field::Json(_)
        | Field::Point(_)
        | Field::Duration(_)
        | Field::Null => Err(InvalidFunctionArgument(
            ScalarFunctionType::Abs.to_string(),
            value,
            0,
        )),
    }
}

pub(crate) fn evaluate_round(
    schema: &Schema,
    arg: &Expression,
    decimals: Option<&Expression>,
    record: &ProcessorRecord,
) -> Result<Field, PipelineError> {
    let value = arg.evaluate(record, schema)?;
    let mut places = 0;
    if let Some(expression) = decimals {
        let field = expression.evaluate(record, schema)?;
        match field {
            Field::UInt(u) => places = u as i32,
            Field::U128(u) => places = u as i32,
            Field::Int(i) => places = i as i32,
            Field::I128(i) => places = i as i32,
            Field::Float(f) => places = f.round().0 as i32,
            Field::Decimal(d) => {
                places = d
                    .to_i32()
                    .ok_or(PipelineError::InvalidCast {
                        from: field,
                        to: FieldType::Decimal,
                    })
                    .unwrap()
            }
            Field::Boolean(_)
            | Field::String(_)
            | Field::Text(_)
            | Field::Date(_)
            | Field::Timestamp(_)
            | Field::Binary(_)
            | Field::Json(_)
            | Field::Point(_)
            | Field::Duration(_)
            | Field::Null => {} // Truncate value to 0 decimals
        }
    }
    let order = OrderedFloat(10.0_f64.powi(places));

    match value {
        Field::UInt(u) => Ok(Field::UInt(u)),
        Field::U128(u) => Ok(Field::U128(u)),
        Field::Int(i) => Ok(Field::Int(i)),
        Field::I128(i) => Ok(Field::I128(i)),
        Field::Float(f) => Ok(Field::Float((f * order).round() / order)),
        Field::Decimal(d) => Ok(Field::Decimal(d.round_dp(places as u32))),
        Field::Null => Ok(Field::Null),
        Field::Boolean(_)
        | Field::String(_)
        | Field::Text(_)
        | Field::Date(_)
        | Field::Timestamp(_)
        | Field::Binary(_)
        | Field::Json(_)
        | Field::Point(_)
        | Field::Duration(_) => Err(InvalidFunctionArgument(
            ScalarFunctionType::Round.to_string(),
            value,
            0,
        )),
    }
}
