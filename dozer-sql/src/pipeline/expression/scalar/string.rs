use crate::arg_str;

use crate::pipeline::errors::PipelineError;

use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use crate::pipeline::expression::scalar::ScalarFunctionType;

use crate::pipeline::expression::arg_utils::validate_arg_type;
use dozer_types::types::{Field, FieldType, Record, Schema};

pub(crate) fn evaluate_ucase(
    schema: &Schema,
    arg: &Expression,
    record: &Record,
) -> Result<Field, PipelineError> {
    let value = arg.evaluate(record, schema)?;
    match value {
        Field::String(s) => Ok(Field::String(s.to_uppercase())),
        Field::Text(t) => Ok(Field::Text(t.to_uppercase())),
        _ => Err(PipelineError::InvalidFunctionArgument(
            ScalarFunctionType::Ucase.to_string(),
            value,
            0,
        )),
    }
}

pub(crate) fn validate_concat(
    arg0: &Expression,
    arg1: &Expression,
    schema: &Schema,
) -> Result<FieldType, PipelineError> {
    let _arg0 = validate_arg_type(
        arg0,
        vec![FieldType::String],
        schema,
        ScalarFunctionType::Concat,
        0,
    )?;
    let _arg1 = validate_arg_type(
        arg1,
        vec![FieldType::String],
        schema,
        ScalarFunctionType::Concat,
        1,
    )?;
    Ok(FieldType::String)
}

pub(crate) fn evaluate_concat(
    schema: &Schema,
    arg0: &Expression,
    arg1: &Expression,
    record: &Record,
) -> Result<Field, PipelineError> {
    let (f0, f1) = (
        arg0.evaluate(record, schema)?,
        arg1.evaluate(record, schema)?,
    );
    let (v0, v1) = (
        arg_str!(f0, ScalarFunctionType::Concat, 0)?,
        arg_str!(f1, ScalarFunctionType::Concat, 1)?,
    );
    Ok(Field::String(v0.to_owned() + v1))
}

pub(crate) fn evaluate_length(
    schema: &Schema,
    arg0: &Expression,
    record: &Record,
) -> Result<Field, PipelineError> {
    let f0 = arg0.evaluate(record, schema)?;
    let v0 = arg_str!(f0, ScalarFunctionType::Concat, 0)?;
    Ok(Field::UInt(v0.len() as u64))
}
