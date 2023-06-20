use crate::arg_str;
use std::fmt::{Display, Formatter};

use crate::pipeline::errors::PipelineError;

use crate::pipeline::expression::execution::{Expression, ExpressionExecutor, ExpressionType};

use crate::pipeline::expression::arg_utils::validate_arg_type;
use crate::pipeline::expression::scalar::common::ScalarFunctionType;

use dozer_types::types::{Field, FieldType, Record, Schema};
use like::{Escape, Like};

pub(crate) fn validate_ucase(
    arg: &Expression,
    schema: &Schema,
) -> Result<ExpressionType, PipelineError> {
    validate_arg_type(
        arg,
        vec![FieldType::String, FieldType::Text],
        schema,
        ScalarFunctionType::Ucase,
        0,
    )
}

pub(crate) fn evaluate_ucase(
    schema: &Schema,
    arg: &Expression,
    record: &Record,
) -> Result<Field, PipelineError> {
    let f = arg.evaluate(record, schema)?;
    let v = arg_str!(f, ScalarFunctionType::Ucase, 0)?;
    let ret = v.to_uppercase();

    Ok(match arg.get_type(schema)?.return_type {
        FieldType::String => Field::String(ret),
        FieldType::UInt
        | FieldType::U128
        | FieldType::Int
        | FieldType::I128
        | FieldType::Float
        | FieldType::Decimal
        | FieldType::Boolean
        | FieldType::Text
        | FieldType::Date
        | FieldType::Timestamp
        | FieldType::Binary
        | FieldType::Json
        | FieldType::Point
        | FieldType::Duration => Field::Text(ret),
    })
}

pub(crate) fn validate_concat(
    args: &[Expression],
    schema: &Schema,
) -> Result<ExpressionType, PipelineError> {
    let mut ret_type = FieldType::String;
    for exp in args {
        let r = validate_arg_type(
            exp,
            vec![FieldType::String, FieldType::Text],
            schema,
            ScalarFunctionType::Concat,
            0,
        )?;
        if matches!(r.return_type, FieldType::Text) {
            ret_type = FieldType::Text;
        }
    }
    Ok(ExpressionType::new(
        ret_type,
        false,
        dozer_types::types::SourceDefinition::Dynamic,
        false,
    ))
}

pub(crate) fn evaluate_concat(
    schema: &Schema,
    args: &[Expression],
    record: &Record,
) -> Result<Field, PipelineError> {
    let mut res_type = FieldType::String;
    let mut res_vec: Vec<String> = Vec::with_capacity(args.len());

    for e in args {
        if matches!(e.get_type(schema)?.return_type, FieldType::Text) {
            res_type = FieldType::Text;
        }
        let f = e.evaluate(record, schema)?;
        let val = arg_str!(f, ScalarFunctionType::Concat, 0)?;
        res_vec.push(val);
    }

    let res_str = res_vec.iter().fold(String::new(), |a, b| a + b.as_str());
    Ok(match res_type {
        FieldType::Text => Field::Text(res_str),
        FieldType::UInt
        | FieldType::U128
        | FieldType::Int
        | FieldType::I128
        | FieldType::Float
        | FieldType::Decimal
        | FieldType::Boolean
        | FieldType::String
        | FieldType::Date
        | FieldType::Timestamp
        | FieldType::Binary
        | FieldType::Json
        | FieldType::Point
        | FieldType::Duration => Field::String(res_str),
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TrimType {
    Trailing,
    Leading,
    Both,
}

impl Display for TrimType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TrimType::Trailing => f.write_str("TRAILING "),
            TrimType::Leading => f.write_str("LEADING "),
            TrimType::Both => f.write_str("BOTH "),
        }
    }
}

pub(crate) fn validate_trim(
    arg: &Expression,
    schema: &Schema,
) -> Result<ExpressionType, PipelineError> {
    validate_arg_type(
        arg,
        vec![FieldType::String, FieldType::Text],
        schema,
        ScalarFunctionType::Concat,
        0,
    )
}

pub(crate) fn evaluate_trim(
    schema: &Schema,
    arg: &Expression,
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
        Some(TrimType::Leading) => arg_value.trim_start_matches::<&[char]>(&v1).to_string(),
        Some(TrimType::Trailing) => arg_value.trim_end_matches::<&[char]>(&v1).to_string(),
        None => arg_value.trim_matches::<&[char]>(&v1).to_string(),
    };

    Ok(match arg.get_type(schema)?.return_type {
        FieldType::String => Field::String(retval),
        FieldType::UInt
        | FieldType::U128
        | FieldType::Int
        | FieldType::I128
        | FieldType::Float
        | FieldType::Decimal
        | FieldType::Boolean
        | FieldType::Text
        | FieldType::Date
        | FieldType::Timestamp
        | FieldType::Binary
        | FieldType::Json
        | FieldType::Point
        | FieldType::Duration => Field::Text(retval),
    })
}

pub(crate) fn get_like_operator_type(
    arg: &Expression,
    pattern: &Expression,
    schema: &Schema,
) -> Result<ExpressionType, PipelineError> {
    validate_arg_type(
        pattern,
        vec![FieldType::String, FieldType::Text],
        schema,
        ScalarFunctionType::Concat,
        0,
    )?;

    validate_arg_type(
        arg,
        vec![FieldType::String, FieldType::Text],
        schema,
        ScalarFunctionType::Concat,
        0,
    )
}

pub(crate) fn evaluate_like(
    schema: &Schema,
    arg: &Expression,
    pattern: &Expression,
    escape: Option<char>,
    record: &Record,
) -> Result<Field, PipelineError> {
    let arg_field = arg.evaluate(record, schema)?;
    let arg_value = arg_str!(arg_field, "LIKE", 0)?;
    let arg_string = arg_value.as_str();

    let pattern_field = pattern.evaluate(record, schema)?;
    let pattern_value = arg_str!(pattern_field, "LIKE", 1)?;
    let pattern_string = pattern_value.as_str();

    if let Some(escape_char) = escape {
        let arg_escape = &arg_string
            .escape(&escape_char.to_string())
            .map_err(|e| PipelineError::InvalidArgument(e.to_string()))?;
        let result = Like::<false>::like(arg_escape.as_str(), pattern_string)
            .map(Field::Boolean)
            .map_err(|e| PipelineError::InvalidArgument(e.to_string()))?;
        return Ok(result);
    }

    let result = Like::<false>::like(arg_string, pattern_string)
        .map(Field::Boolean)
        .map_err(|e| PipelineError::InvalidArgument(e.to_string()))?;
    Ok(result)
}

pub(crate) fn evaluate_to_char(
    schema: &Schema,
    arg: &Expression,
    pattern: &Expression,
    record: &Record,
) -> Result<Field, PipelineError> {
    let arg_field = arg.evaluate(record, schema)?;

    let pattern_field = pattern.evaluate(record, schema)?;
    let pattern_value = arg_str!(pattern_field, "TO_CHAR", 0)?;

    let output = match arg_field {
        Field::Timestamp(value) => value.format(pattern_value.as_str()).to_string(),
        Field::Date(value) => value.format(pattern_value.as_str()).to_string(),
        Field::Null => return Ok(Field::Null),
        _ => {
            return Err(PipelineError::InvalidArgument(format!(
                "TO_CHAR({}, ...)",
                arg_field
            )))
        }
    };

    Ok(Field::String(output))
}
