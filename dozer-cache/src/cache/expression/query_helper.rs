use crate::errors::{validate_query, QueryValidationError, QueryValidationError::*};
use dozer_types::serde_json::{self, Value};

use super::super::expression::{FilterExpression, Operator};
use super::{SortDirection, SortOption};

fn validate_field_name(key: &str) -> Result<(), QueryValidationError> {
    if !key.eq("_")
        && key
            .chars()
            .filter(|x| !x.eq(&'_'))
            .all(|x| x.is_ascii_alphanumeric())
    {
        Ok(())
    } else {
        Err(UnexpectedCharacter(key.to_owned()))
    }
}

fn construct_simple_expression(
    key: String,
    op: Operator,
    value: Value,
) -> Result<FilterExpression, QueryValidationError> {
    validate_query(
        if let Value::String(string) = &value {
            string.chars().all(|x| x.is_ascii())
        } else {
            true
        },
        SpecialCharacterError,
    )?;
    validate_query(
        if let Value::Object(object) = &value {
            !object.is_empty()
        } else {
            true
        },
        EmptyObjectAsValue,
    )?;

    validate_query(
        if let Value::Array(array) = &value {
            !array.is_empty()
        } else {
            true
        },
        EmptyArrayAsValue,
    )?;
    let expression = FilterExpression::Simple(key, op, value);
    Ok(expression)
}

pub fn simple_expression(
    key: String,
    value: Value,
) -> Result<FilterExpression, QueryValidationError> {
    validate_field_name(&key)?;
    match value {
        Value::Object(pairs) => {
            validate_query(pairs.len() <= 1, MoreThanOneStmt)?;
            let (inner_key, scalar_value) = pairs.into_iter().next().ok_or(EmptyObjectAsValue)?;
            let operator: Operator =
                Operator::convert_str(&inner_key).ok_or(UnidentifiedOperator(inner_key))?;

            let expression = construct_simple_expression(key, operator, scalar_value)?;
            Ok(expression)
        }
        Value::Number(_) | Value::String(_) | Value::Bool(_) | Value::Null => {
            let expression = construct_simple_expression(key, Operator::EQ, value)?;
            Ok(expression)
        }
        Value::Array(_) => Err(InvalidExpression),
    }
}

pub fn and_expression(conditions: Value) -> Result<FilterExpression, QueryValidationError> {
    let Value::Array(conditions) = conditions else {
        return Err(InvalidAndExpression);
    };

    let mut expressions = vec![];
    for condition in conditions {
        let expr: FilterExpression =
            serde_json::from_value(condition).map_err(|_| InvalidExpression)?;
        expressions.push(expr);
    }

    validate_query(expressions.len() >= 2, InvalidAndExpression)?;

    Ok(FilterExpression::And(expressions))
}

pub fn sort_option(key: String, value: Value) -> Result<SortOption, QueryValidationError> {
    validate_field_name(&key)?;
    let Value::String(direction) = value else {
        return Err(OrderValueNotString);
    };
    let direction = SortDirection::convert_str(&direction).ok_or(UnidentifiedOrder(direction))?;
    Ok(SortOption::new(key, direction))
}
