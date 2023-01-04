use crate::pipeline::errors::PipelineError::InvalidFunctionArgumentType;
use crate::pipeline::errors::{FieldTypes, PipelineError};
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use crate::pipeline::expression::scalar::ScalarFunctionType;
use dozer_types::types::{FieldType, Schema};

pub(crate) fn validate_arg_type(
    arg: &Expression,
    expected: Vec<FieldType>,
    schema: &Schema,
    fct: ScalarFunctionType,
    idx: usize,
) -> Result<FieldType, PipelineError> {
    let arg_t = arg.get_type(schema)?;
    if !expected.contains(&arg_t) {
        Err(InvalidFunctionArgumentType(
            fct.to_string(),
            arg_t,
            FieldTypes::new(expected),
            idx,
        ))
    } else {
        Ok(arg_t)
    }
}

#[macro_export]
macro_rules! argv {
    ($arr: expr, $idx: expr, $fct: expr) => {
        match $arr.get($idx) {
            Some(v) => Ok(v),
            _ => Err(PipelineError::NotEnoughArguments($fct.to_string())),
        }
    };
}

#[macro_export]
macro_rules! arg_str {
    ($field: expr, $fct: expr, $idx: expr) => {
        match $field.as_string() {
            Some(e) => Ok(e),
            _ => Err(PipelineError::InvalidFunctionArgument(
                $fct.to_string(),
                $field,
                $idx,
            )),
        }
    };
}

#[macro_export]
macro_rules! arg_uint {
    ($field: expr, $fct: expr, $idx: expr) => {
        match $field.as_uint() {
            Some(e) => Ok(e),
            _ => Err(PipelineError::InvalidFunctionArgument(
                $fct.to_string(),
                $field,
                $idx,
            )),
        }
    };
}

#[macro_export]
macro_rules! arg_int {
    ($field: expr, $fct: expr, $idx: expr) => {
        match $field.as_int() {
            Some(e) => Ok(e),
            _ => Err(PipelineError::InvalidFunctionArgument(
                $fct.to_string(),
                $field,
                $idx,
            )),
        }
    };
}

#[macro_export]
macro_rules! arg_float {
    ($field: expr, $fct: expr, $idx: expr) => {
        match $field.as_float() {
            Some(e) => Ok(e),
            _ => Err(PipelineError::InvalidFunctionArgument(
                $fct.to_string(),
                $field,
                $idx,
            )),
        }
    };
}

#[macro_export]
macro_rules! arg_binary {
    ($field: expr, $fct: expr, $idx: expr) => {
        match $field.as_binary() {
            Some(e) => Ok(e),
            _ => Err(PipelineError::InvalidFunctionArgument(
                $fct.to_string(),
                $field,
                $idx,
            )),
        }
    };
}

#[macro_export]
macro_rules! arg_decimal {
    ($field: expr, $fct: expr, $idx: expr) => {
        match $field.as_decimal() {
            Some(e) => Ok(e),
            _ => Err(PipelineError::InvalidFunctionArgument(
                $fct.to_string(),
                $field,
                $idx,
            )),
        }
    };
}

#[macro_export]
macro_rules! arg_timestamp {
    ($field: expr, $fct: expr, $idx: expr) => {
        match $field.as_timestamp() {
            Some(e) => Ok(e),
            _ => Err(PipelineError::InvalidFunctionArgument(
                $fct.to_string(),
                $field,
                $idx,
            )),
        }
    };
}

#[macro_export]
macro_rules! arg_date {
    ($field: expr, $fct: expr, $idx: expr) => {
        match $field.as_date() {
            Some(e) => Ok(e),
            _ => Err(PipelineError::InvalidFunctionArgument(
                $fct.to_string(),
                $field,
                $idx,
            )),
        }
    };
}
