use anyhow::{bail, ensure, Context, Ok};
use dozer_cache::cache::expression::{Comparator, Expression, Operator};
use dozer_types::json_value_to_field;
use serde_json::Value;

fn string_to_operator(input: String) -> Option<Operator> {
    match input.to_lowercase().as_str() {
        "or" => Option::Some(Operator::OR),
        "and" => Option::Some(Operator::AND),
        _ => Option::None,
    }
}

fn string_to_comparator(input: String) -> Option<Comparator> {
    match input.to_lowercase().as_str() {
        "eq" => Option::Some(Comparator::EQ),
        "gt" => Option::Some(Comparator::GT),
        "gte" => Option::Some(Comparator::GTE),
        "lt" => Option::Some(Comparator::LT),
        "lte" => Option::Some(Comparator::LTE),
        _ => Option::None,
    }
}

fn value_to_simple_expression(
    key: String,
    comparator: Comparator,
    value: Value,
) -> anyhow::Result<Expression> {
    let field = json_value_to_field(value)?;
    let expression = Expression::Simple(key, comparator, field);
    Ok(expression)
}

fn value_to_composite_expression(operator: Operator, value: Value) -> anyhow::Result<Expression> {
    ensure!(
        Value::is_array(&value),
        "Composite must follow by array"
    );
    let array_condition = value_to_expression(value)?;
    let expression = Expression::Composite(
        operator,
        Box::new(array_condition[0].to_owned()),
        Box::new(array_condition[1].to_owned()),
    );
    Ok(expression)
}

pub fn value_to_expression(input: Value) -> anyhow::Result<Vec<Expression>, anyhow::Error> {
    match input {
        Value::Array(array_value) => {
            let mut result: Vec<Expression> = Vec::new();
            for value in array_value {
                let expression = value_to_expression(value)?;
                result.extend(expression);
            }
            Ok(result)
        }
        Value::Object(pairs) => {
            let mut result: Vec<Expression> = Vec::new();
            // check if match any comparator:
            for pair in pairs {
                let pair_value: Value = pair.1;
                let pair_key = pair.0;
                let parsed_operator = string_to_operator(pair_key.to_owned());
                if parsed_operator.is_some() {
                    let expression = value_to_composite_expression(
                        parsed_operator.unwrap(),
                        pair_value.to_owned(),
                    )?;
                    result.push(expression);
                    continue;
                }
                // extract inner key
                let comparator_key = pair_value.as_object().cloned();
                let keys = comparator_key.context("Invalid Expression")?;
                let key = keys.keys().next().cloned().context("Invalid Expression")?;
                let parsed_comparator =
                    string_to_comparator(key.to_owned()).context("Comparator does not match")?;
                let scalar_value = pair_value
                    .get(key.to_owned())
                    .context(format!("scalar value by key {:?} is empty", key))?;
                let expression = value_to_simple_expression(
                    pair_key.to_owned(),
                    parsed_comparator,
                    scalar_value.to_owned(),
                )?;
                result.push(expression);
                continue;
            }
            Ok(result)
        }
        Value::Bool(_) => bail!("Invalid Expression"),
        Value::Number(_) => bail!("Invalid Expression"),
        Value::String(_) => bail!("Invalid Expression"),
        Value::Null => Ok(vec![Expression::None]),
    }
}
