use crate::errors::{validate_query, QueryValidationError, QueryValidationError::*};
use dozer_types::serde_json::{self};
use dozer_types::serde_json::{json, Value};

use super::super::expression::{FilterExpression, Operator};

fn construct_simple_expression(
    key: &str,
    op: Operator,
    value: Value,
) -> Result<FilterExpression, QueryValidationError> {
    validate_query(
        value.to_string().chars().all(|x| x.is_ascii()),
        SpecialCharacterError,
    )?;
    validate_query(
        !(value.is_object() && value == json!({})),
        EmptyObjectAsValue,
    )?;

    validate_query(
        !(value.is_array() && value.as_array().unwrap().is_empty()),
        EmptyArrayAsValue,
    )?;
    let expression = FilterExpression::Simple(key.to_owned(), op, value);
    Ok(expression)
}

pub fn simple_expression(
    key: &String,
    value: Value,
) -> Result<FilterExpression, QueryValidationError> {
    validate_query(
        !key.eq("_")
            && key
                .chars()
                .filter(|x| !x.eq(&'_'))
                .all(|x| x.is_ascii_alphanumeric()),
        UnexpectedCharacter(key.clone()),
    )?;
    match value {
        Value::Object(pairs) => {
            validate_query(!pairs.is_empty(), EmptyObjectAsValue)?;
            validate_query(pairs.len() == 1, MoreThanOneStmt)?;
            let inner_key = pairs.keys().next().unwrap();
            let operator: Operator = Operator::convert_str(inner_key)
                .map_or(Err(UnidentifiedOperator(inner_key.clone())), Ok)?;
            let scalar_value = pairs.get(inner_key).unwrap();

            let expression = construct_simple_expression(key, operator, scalar_value.to_owned())?;
            Ok(expression)
        }
        Value::Number(_) | Value::String(_) | Value::Bool(_) | Value::Null => {
            let expression = construct_simple_expression(key, Operator::EQ, value.to_owned())?;
            Ok(expression)
        }
        Value::Array(_) => Err(InvalidExpression),
    }
}

pub fn and_expression(conditions: Value) -> Result<FilterExpression, QueryValidationError> {
    let conditions = conditions
        .as_array()
        .map_or(Err(InvalidAndExpression), Ok)?;

    let mut expressions = vec![];
    for c in conditions {
        let expr: FilterExpression =
            serde_json::from_value(c.to_owned()).map_err(|_| InvalidExpression)?;
        expressions.push(expr);
    }

    validate_query(expressions.len() >= 2, InvalidAndExpression)?;

    Ok(FilterExpression::And(expressions))
}
