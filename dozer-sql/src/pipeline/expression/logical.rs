use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use dozer_types::types::{Field, Record, Schema};

pub fn evaluate_and(
    schema: &Schema,
    left: &Expression,
    right: &Expression,
    record: &Record,
) -> Result<Field, PipelineError> {
    match left.evaluate(record, schema)? {
        Field::Boolean(true) => match right.evaluate(record, schema)? {
            Field::Boolean(true) => Ok(Field::Boolean(true)),
            Field::Boolean(false) => Ok(Field::Boolean(false)),
            Field::Null => Ok(Field::Boolean(false)),
            not_supported_field => Err(PipelineError::InvalidType(
                not_supported_field,
                "AND".to_string(),
            )),
        },
        Field::Boolean(false) => match right.evaluate(record, schema)? {
            Field::Boolean(true) => Ok(Field::Boolean(false)),
            Field::Boolean(false) => Ok(Field::Boolean(false)),
            Field::Null => Ok(Field::Boolean(false)),
            not_supported_field => Err(PipelineError::InvalidType(
                not_supported_field,
                "AND".to_string(),
            )),
        },
        Field::Null => Ok(Field::Boolean(false)),
        not_supported_field => Err(PipelineError::InvalidType(
            not_supported_field,
            "AND".to_string(),
        )),
    }
}

pub fn evaluate_or(
    schema: &Schema,
    left: &Expression,
    right: &Expression,
    record: &Record,
) -> Result<Field, PipelineError> {
    match left.evaluate(record, schema)? {
        Field::Boolean(true) => match right.evaluate(record, schema)? {
            Field::Boolean(false) => Ok(Field::Boolean(true)),
            Field::Boolean(true) => Ok(Field::Boolean(true)),
            Field::Null => Ok(Field::Boolean(true)),
            not_supported_field => Err(PipelineError::InvalidType(
                not_supported_field,
                "OR".to_string(),
            )),
        },
        Field::Boolean(false) | Field::Null => match right.evaluate(record, schema)? {
            Field::Boolean(false) => Ok(Field::Boolean(false)),
            Field::Boolean(true) => Ok(Field::Boolean(true)),
            Field::Null => Ok(Field::Boolean(false)),
            not_supported_field => Err(PipelineError::InvalidType(
                not_supported_field,
                "OR".to_string(),
            )),
        },
        not_supported_field => Err(PipelineError::InvalidType(
            not_supported_field,
            "OR".to_string(),
        )),
    }
}

pub fn evaluate_not(
    schema: &Schema,
    value: &Expression,
    record: &Record,
) -> Result<Field, PipelineError> {
    let value_p = value.evaluate(record, schema)?;

    match value_p {
        Field::Boolean(value_v) => Ok(Field::Boolean(!value_v)),
        Field::Null => Ok(Field::Null),
        not_supported_field => Err(PipelineError::InvalidType(
            not_supported_field,
            "NOT".to_string(),
        )),
    }
}
