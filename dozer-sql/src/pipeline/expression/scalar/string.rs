use crate::arg_str;
use sqlparser::ast::CopyTarget::File;

use crate::pipeline::errors::PipelineError;

use crate::pipeline::expression::execution::{Expression, ExpressionExecutor, TrimType};

use crate::pipeline::expression::arg_utils::validate_arg_type;
use crate::pipeline::expression::scalar::common::ScalarFunctionType;
use dozer_types::types::{Field, FieldType, Record, Schema};

pub(crate) fn validate_ucase(
    arg: &Expression,
    schema: &Schema,
) -> Result<FieldType, PipelineError> {
    Ok(validate_arg_type(
        arg,
        vec![FieldType::String, FieldType::Text],
        schema,
        ScalarFunctionType::Ucase,
        0,
    )?)
}

pub(crate) fn evaluate_ucase(
    schema: &Schema,
    arg: &Expression,
    record: &Record,
) -> Result<Field, PipelineError> {
    let f = arg.evaluate(record, schema)?;
    let v = arg_str!(f, ScalarFunctionType::Ucase, 0)?;
    let ret = v.to_uppercase();

    Ok(match arg.get_type(schema)? {
        FieldType::String => Field::String(ret),
        _ => Field::Text(ret),
    })
}

pub(crate) fn validate_concat(
    arg0: &Expression,
    arg1: &Expression,
    schema: &Schema,
) -> Result<FieldType, PipelineError> {
    let _arg0 = validate_arg_type(
        arg0,
        vec![FieldType::String, FieldType::Text],
        schema,
        ScalarFunctionType::Concat,
        0,
    )?;
    let _arg1 = validate_arg_type(
        arg1,
        vec![FieldType::String, FieldType::Text],
        schema,
        ScalarFunctionType::Concat,
        1,
    )?;

    Ok(match (arg0.get_type(schema)?, arg1.get_type(schema)?) {
        (FieldType::String, FieldType::String) => FieldType::String,
        _ => FieldType::Text,
    })
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
    let ret_val = v0.to_owned() + v1;

    Ok(match (arg0.get_type(schema)?, arg1.get_type(schema)?) {
        (FieldType::String, FieldType::String) => Field::String(ret_val),
        _ => Field::Text(ret_val),
    })
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

pub(crate) fn validate_trim(arg: &Expression, schema: &Schema) -> Result<FieldType, PipelineError> {
    let ret_type = validate_arg_type(
        arg,
        vec![FieldType::String, FieldType::Text],
        schema,
        ScalarFunctionType::Concat,
        0,
    )?;
    Ok(ret_type)
}

pub(crate) fn evaluate_trim(
    schema: &Schema,
    arg: &Box<Expression>,
    what: &Option<Box<Expression>>,
    typ: &Option<TrimType>,
    record: &Record,
) -> Result<Field, PipelineError> {
    let arg_field = arg.evaluate(record, schema)?;
    let arg_value = arg_str!(arg_field, "TRIM", 0)?;

    let v1: Vec<_> = match what {
        Some(e) => {
            let f = e.evaluate(record, schema)?;
            arg_str!(f, "TRIM", 1)?.chars().collect()
        }
        _ => vec![' '],
    };

    let retval = match typ {
        Some(TrimType::Both) => arg_value.trim_matches::<&[char]>(&v1).to_string(),
        Some(TrimType::Leading) => arg_value.trim_left_matches::<&[char]>(&v1).to_string(),
        Some(TrimType::Trailing) => arg_value.trim_right_matches::<&[char]>(&v1).to_string(),
        None => arg_value.trim_matches::<&[char]>(&v1).to_string(),
    };

    Ok(match arg.get_type(schema)? {
        FieldType::String => Field::String(retval),
        _ => Field::Text(retval),
    })
}
