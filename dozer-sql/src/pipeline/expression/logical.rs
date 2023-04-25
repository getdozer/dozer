use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use dozer_types::types::{Field, Record, Schema};

pub fn evaluate_and(
    schema: &Schema,
    left: &Expression,
    right: &Expression,
    record: &Record,
) -> Result<Field, PipelineError> {
    let l_field = left.evaluate(record, schema)?;
    let r_field = right.evaluate(record, schema)?;
    match l_field {
        Field::Boolean(true) => match r_field {
            Field::Boolean(true) => Ok(Field::Boolean(true)),
            Field::Boolean(false) => Ok(Field::Boolean(false)),
            Field::Null => Ok(Field::Boolean(false)),
            Field::UInt(_)
            | Field::U128(_)
            | Field::Int(_)
            | Field::I128(_)
            | Field::Float(_)
            | Field::String(_)
            | Field::Text(_)
            | Field::Binary(_)
            | Field::Decimal(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(PipelineError::InvalidType(r_field, "AND".to_string())),
        },
        Field::Boolean(false) => match r_field {
            Field::Boolean(true) => Ok(Field::Boolean(false)),
            Field::Boolean(false) => Ok(Field::Boolean(false)),
            Field::Null => Ok(Field::Boolean(false)),
            Field::UInt(_)
            | Field::U128(_)
            | Field::Int(_)
            | Field::I128(_)
            | Field::Float(_)
            | Field::String(_)
            | Field::Text(_)
            | Field::Binary(_)
            | Field::Decimal(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(PipelineError::InvalidType(r_field, "AND".to_string())),
        },
        Field::Null => Ok(Field::Boolean(false)),
        Field::UInt(_)
        | Field::U128(_)
        | Field::Int(_)
        | Field::I128(_)
        | Field::Float(_)
        | Field::String(_)
        | Field::Text(_)
        | Field::Binary(_)
        | Field::Decimal(_)
        | Field::Timestamp(_)
        | Field::Date(_)
        | Field::Bson(_)
        | Field::Point(_)
        | Field::Duration(_) => Err(PipelineError::InvalidType(l_field, "AND".to_string())),
    }
}

pub fn evaluate_or(
    schema: &Schema,
    left: &Expression,
    right: &Expression,
    record: &Record,
) -> Result<Field, PipelineError> {
    let l_field = left.evaluate(record, schema)?;
    let r_field = right.evaluate(record, schema)?;
    match l_field {
        Field::Boolean(true) => match r_field {
            Field::Boolean(false) => Ok(Field::Boolean(true)),
            Field::Boolean(true) => Ok(Field::Boolean(true)),
            Field::Null => Ok(Field::Boolean(true)),
            Field::UInt(_)
            | Field::U128(_)
            | Field::Int(_)
            | Field::I128(_)
            | Field::Float(_)
            | Field::String(_)
            | Field::Text(_)
            | Field::Binary(_)
            | Field::Decimal(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(PipelineError::InvalidType(r_field, "OR".to_string())),
        },
        Field::Boolean(false) | Field::Null => match right.evaluate(record, schema)? {
            Field::Boolean(false) => Ok(Field::Boolean(false)),
            Field::Boolean(true) => Ok(Field::Boolean(true)),
            Field::Null => Ok(Field::Boolean(false)),
            Field::UInt(_)
            | Field::U128(_)
            | Field::Int(_)
            | Field::I128(_)
            | Field::Float(_)
            | Field::String(_)
            | Field::Text(_)
            | Field::Binary(_)
            | Field::Decimal(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Bson(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(PipelineError::InvalidType(r_field, "OR".to_string())),
        },
        Field::UInt(_)
        | Field::U128(_)
        | Field::Int(_)
        | Field::I128(_)
        | Field::Float(_)
        | Field::String(_)
        | Field::Text(_)
        | Field::Binary(_)
        | Field::Decimal(_)
        | Field::Timestamp(_)
        | Field::Date(_)
        | Field::Bson(_)
        | Field::Point(_)
        | Field::Duration(_) => Err(PipelineError::InvalidType(l_field, "OR".to_string())),
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
        Field::UInt(_)
        | Field::U128(_)
        | Field::Int(_)
        | Field::I128(_)
        | Field::Float(_)
        | Field::String(_)
        | Field::Text(_)
        | Field::Binary(_)
        | Field::Decimal(_)
        | Field::Timestamp(_)
        | Field::Date(_)
        | Field::Bson(_)
        | Field::Point(_)
        | Field::Duration(_) => Err(PipelineError::InvalidType(value_p, "NOT".to_string())),
    }
}
