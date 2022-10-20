use std::str::FromStr;

use super::super::expression::{FilterExpression, Operator};
use anyhow::{bail, ensure, Context, Ok};
use dozer_types::serde_json::Value;
use dozer_types::{json_value_to_field, serde_json};

pub fn is_combinator(input: String) -> bool {
    vec!["$or", "$and"].contains(&input.to_lowercase().as_str())
}

fn construct_simple_expression(
    key: String,
    op: Operator,
    value: Value,
) -> anyhow::Result<FilterExpression> {
    ensure!(
        value.to_string().chars().all(|x| x.is_ascii()),
        "Scalar value cannot contain special character"
    );
    let field = json_value_to_field(value)?;
    let expression = FilterExpression::Simple(key, op, field);
    Ok(expression)
}

pub fn value_to_simple_exp(key: String, value: Value) -> anyhow::Result<FilterExpression> {
    ensure!(
        !key.eq("_")
            && key
                .chars()
                .filter(|x| !x.eq(&'_'))
                .all(|x| x.is_ascii_alphanumeric()),
        "Key cannot contains special character"
    );
    match value {
        Value::Object(pairs) => {
            ensure!(!pairs.is_empty(), "Empty object input");
            ensure!(pairs.len() == 1, "Simple expression can only accept 1 stmt");
            let inner_key = pairs
                .keys()
                .next()
                .context("Missing key in Simple expression")?;
            let operator: Operator = Operator::from_str(inner_key)?;
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

pub fn value_to_composite_expression(
    comparator: String,
    value: Value,
) -> anyhow::Result<FilterExpression> {
    let array = value.as_array().context("Composite must follow by array")?;
    let array_condition: Vec<FilterExpression> = array
        .iter()
        .map(|c| -> anyhow::Result<FilterExpression> {
            let result: FilterExpression = serde_json::from_value(c.to_owned())?;
            Ok(result)
        })
        .filter_map(|r| r.ok())
        .collect();

    let exp = match comparator.as_str() {
        "$or" => bail!("Or not supported"),
        "$and" => {
            ensure!(
                array_condition.len() > 1,
                "AND require at least 2 valid conditions input"
            );

            array_condition
                .iter()
                .enumerate()
                .try_fold(
                    FilterExpression::And(
                        Box::new(array_condition[0].to_owned()),
                        Box::new(array_condition[1].to_owned()),
                    ),
                    |acc, (index, curr)| {
                        if index < 2 {
                            Ok(acc)
                        } else {
                            Ok(FilterExpression::And(
                                Box::new(acc),
                                Box::new(curr.to_owned()),
                            ))
                        }
                    },
                )
                .context("Cannot parse AND expression")?
        }
        _ => bail!("unrecoginzed operator"),
    };

    Ok(exp)
}
