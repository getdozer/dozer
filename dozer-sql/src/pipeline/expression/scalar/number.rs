use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::InvalidFunctionArgument;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use crate::pipeline::expression::scalar::ScalarFunctionType;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, Record, Schema};
use num_traits::Float;

pub(crate) fn evaluate_abs(
    schema: &Schema,
    arg: &Expression,
    record: &Record,
) -> Result<Field, PipelineError> {
    let value = arg.evaluate(record, schema)?;
    match value {
        Field::Int(i) => Ok(Field::Int(i.abs())),
        Field::Float(f) => Ok(Field::Float(f.abs())),
        _ => Err(PipelineError::InvalidFunctionArgument(
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
    record: &Record,
) -> Result<Field, PipelineError> {
    let value = arg.evaluate(record, schema)?;
    let mut places = 0;
    if let Some(expression) = decimals {
        match expression.evaluate(record, schema)? {
            Field::Int(i) => places = i as i32,
            Field::Float(f) => places = f.round().0 as i32,
            _ => {} // Truncate value to 0 decimals
        }
    }
    let order = OrderedFloat(10.0_f64.powi(places));

    match value {
        Field::Int(i) => Ok(Field::Int(i)),
        Field::Float(f) => Ok(Field::Float((f * order).round() / order)),
        Field::Decimal(_) => Err(PipelineError::InvalidOperandType("ROUND()".to_string())),
        _ => Err(InvalidFunctionArgument(
            ScalarFunctionType::Round.to_string(),
            value,
            0,
        )),
    }
}
