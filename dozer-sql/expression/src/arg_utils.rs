use std::fmt::Display;
use std::ops::Range;

use crate::error::Error;
use crate::execution::{Expression, ExpressionType};
use dozer_types::chrono::{DateTime, FixedOffset};
use dozer_types::types::{DozerPoint, Field, FieldType, Schema};

pub fn validate_one_argument(
    args: &[Expression],
    schema: &Schema,
    function_name: impl Display,
) -> Result<ExpressionType, Error> {
    validate_num_arguments(1..2, args.len(), function_name)?;
    args[0].get_type(schema)
}

pub fn validate_two_arguments(
    args: &[Expression],
    schema: &Schema,
    function_name: impl Display,
) -> Result<(ExpressionType, ExpressionType), Error> {
    validate_num_arguments(2..3, args.len(), function_name)?;
    let arg1 = args[0].get_type(schema)?;
    let arg2 = args[1].get_type(schema)?;
    Ok((arg1, arg2))
}

pub fn validate_num_arguments(
    expected: Range<usize>,
    actual: usize,
    function_name: impl Display,
) -> Result<(), Error> {
    if !expected.contains(&actual) {
        Err(Error::InvalidNumberOfArguments {
            function_name: function_name.to_string(),
            expected,
            actual,
        })
    } else {
        Ok(())
    }
}

pub fn validate_arg_type(
    arg: &Expression,
    expected: Vec<FieldType>,
    schema: &Schema,
    function_name: impl Display,
    argument_index: usize,
) -> Result<ExpressionType, Error> {
    let arg_t = arg.get_type(schema)?;
    if !expected.contains(&arg_t.return_type) {
        Err(Error::InvalidFunctionArgumentType {
            function_name: function_name.to_string(),
            argument_index,
            actual: arg_t.return_type,
            expected,
        })
    } else {
        Ok(arg_t)
    }
}

pub fn extract_uint(
    field: Field,
    function_name: impl Display,
    argument_index: usize,
) -> Result<u64, Error> {
    if let Some(value) = field.to_uint() {
        Ok(value)
    } else {
        Err(Error::InvalidFunctionArgument {
            function_name: function_name.to_string(),
            argument_index,
            argument: field,
        })
    }
}

pub fn extract_float(
    field: Field,
    function_name: impl Display,
    argument_index: usize,
) -> Result<f64, Error> {
    if let Some(value) = field.to_float() {
        Ok(value)
    } else {
        Err(Error::InvalidFunctionArgument {
            function_name: function_name.to_string(),
            argument_index,
            argument: field,
        })
    }
}

pub fn extract_point(
    field: Field,
    function_name: impl Display,
    argument_index: usize,
) -> Result<DozerPoint, Error> {
    if let Some(value) = field.to_point() {
        Ok(value)
    } else {
        Err(Error::InvalidFunctionArgument {
            function_name: function_name.to_string(),
            argument_index,
            argument: field,
        })
    }
}

pub fn extract_timestamp(
    field: Field,
    function_name: impl Display,
    argument_index: usize,
) -> Result<DateTime<FixedOffset>, Error> {
    if let Some(value) = field.to_timestamp() {
        Ok(value)
    } else {
        Err(Error::InvalidFunctionArgument {
            function_name: function_name.to_string(),
            argument_index,
            argument: field,
        })
    }
}
