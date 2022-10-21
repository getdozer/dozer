use super::super::expression::{FilterExpression, Operator};
use anyhow::{bail, ensure, Context, Ok};
use dozer_types::serde_json::Value;
use dozer_types::serde_json::{self, json};

fn construct_simple_expression(
    key: String,
    op: Operator,
    value: Value,
) -> anyhow::Result<FilterExpression> {
    ensure!(
        value.to_string().chars().all(|x| x.is_ascii()),
        "Scalar value cannot contain special character"
    );
    ensure!(
        !(value.is_object() && value == json!({})),
        "empty object passed as value"
    );

    ensure!(
        !(value.is_array() && value.as_array().context("array expected")?.is_empty()),
        "empty array passed as value"
    );
    let expression = FilterExpression::Simple(key, op, value);
    Ok(expression)
}

pub fn simple_expression(key: String, value: Value) -> anyhow::Result<FilterExpression> {
    ensure!(
        !key.eq("_")
            && key
                .chars()
                .filter(|x| !x.eq(&'_'))
                .all(|x| x.is_ascii_alphanumeric()),
        format!("unexpected character : {}", key)
    );
    match value {
        Value::Object(pairs) => {
            ensure!(
                !pairs.is_empty(),
                format!("Unexpected object in query, ({}, {:?})", key, pairs)
            );
            ensure!(pairs.len() == 1, "Simple expression can only accept 1 stmt");
            let inner_key = pairs
                .keys()
                .next()
                .context("Missing key in Simple expression")?;
            let operator: Operator = Operator::from_str(inner_key)
                .context(format!("unidentified operator {}", inner_key))?;
            let scalar_value = pairs
                .get(inner_key)
                .context(format!("scalar value by key {:?} is empty", inner_key))?;

            let expression = construct_simple_expression(key, operator, scalar_value.to_owned())?;
            Ok(expression)
        }
        Value::Number(_) | Value::String(_) | Value::Bool(_) | Value::Null => {
            let expression = construct_simple_expression(key, Operator::EQ, value.to_owned())?;
            Ok(expression)
        }
        Value::Array(_) => {
            bail!("Invalid Simple Expression")
        }
    }
}

pub fn and_expression(conditions: Value) -> anyhow::Result<FilterExpression> {
    let conditions = conditions.as_array().context("$and expects an array")?;

    let mut expressions = vec![];
    for c in conditions {
        let expr: FilterExpression = serde_json::from_value(c.to_owned())?;
        expressions.push(expr);
    }

    if expressions.len() < 2 {
        bail!("$and expects more than one condition.")
    }

    Ok(FilterExpression::And(expressions))
}
