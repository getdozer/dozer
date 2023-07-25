use crate::pipeline::errors::PipelineError::InvalidFunctionArgumentType;
use crate::pipeline::errors::{FieldTypes, PipelineError};
use crate::pipeline::expression::execution::{Expression, ExpressionType};
use crate::pipeline::expression::scalar::common::ScalarFunctionType;
use dozer_types::types::{FieldType, Schema};

pub(crate) fn validate_arg_type(
    arg: &Expression,
    expected: Vec<FieldType>,
    schema: &Schema,
    fct: ScalarFunctionType,
    idx: usize,
) -> Result<ExpressionType, PipelineError> {
    let arg_t = arg.get_type(schema)?;
    if !expected.contains(&arg_t.return_type) {
        Err(InvalidFunctionArgumentType(
            fct.to_string(),
            arg_t.return_type,
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
        match $field.to_string() {
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
        match $field.to_uint() {
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
        match $field.to_int() {
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
        match $field.to_float() {
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
        match $field.to_binary() {
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
        match $field.to_decimal() {
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
        match $field.to_timestamp() {
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
        match $field.to_date() {
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
macro_rules! arg_point {
    ($field: expr, $fct: expr, $idx: expr) => {
        match $field.to_point() {
            Some(e) => Ok(e),
            _ => Err(PipelineError::InvalidFunctionArgument(
                $fct.to_string(),
                $field,
                $idx,
            )),
        }
    };
}
