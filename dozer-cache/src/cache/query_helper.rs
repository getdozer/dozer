use super::expression::{FilterExpression, Operator};
use anyhow::{bail, ensure, Context, Ok};
use dozer_types::json_value_to_field;
use dozer_types::serde_json::Value;

fn is_combinator(input: String) -> bool {
    vec!["$or", "$and"].contains(&input.to_lowercase().as_str())
}
fn string_to_operator(input: String) -> Option<Operator> {
    match input.to_lowercase().as_str() {
        "$eq" => Some(Operator::EQ),
        "$gt" => Some(Operator::GT),
        "$gte" => Some(Operator::GTE),
        "$lt" => Some(Operator::LT),
        "$lte" => Some(Operator::LTE),
        "$contains" => Some(Operator::Contains),
        "$matchesany" => Some(Operator::MatchesAny),
        "$matchesall" => Some(Operator::MatchesAll),
        _ => Option::None,
    }
}

fn value_to_simple_expression(
    key: String,
    op: Operator,
    value: Value,
) -> anyhow::Result<FilterExpression> {
    let field = json_value_to_field(value)?;
    let expression = FilterExpression::Simple(key, op, field);
    Ok(expression)
}

fn value_to_composite_expression(
    comparator: String,
    value: Value,
) -> anyhow::Result<FilterExpression> {
    ensure!(Value::is_array(&value), "Composite must follow by array");
    let array_condition = value_to_expression(value)?;
    let exp = match comparator.as_str() {
        "$or" => bail!("Or not supported"),
        "$and" => FilterExpression::And(
            Box::new(array_condition[0].to_owned()),
            Box::new(array_condition[1].to_owned()),
        ),
        _ => bail!("unrecoginzed operator"),
    };

    Ok(exp)
}

pub fn value_to_expression(input: Value) -> anyhow::Result<Vec<FilterExpression>> {
    match input {
        Value::Array(array_value) => {
            let mut result: Vec<FilterExpression> = Vec::new();
            for value in array_value {
                let expression = value_to_expression(value)?;
                result.extend(expression);
            }
            Ok(result)
        }
        Value::Object(pairs) => {
            let mut result: Vec<FilterExpression> = Vec::new();
            // check if match any Operator:
            for pair in pairs {
                let pair_value: Value = pair.1;
                let pair_key = pair.0;

                if is_combinator(pair_key.to_owned()) {
                    let expression =
                        value_to_composite_expression(pair_key, pair_value.to_owned())?;
                    result.push(expression);
                    continue;
                }
                // extract inner key
                if let Value::Object(keys) = pair_value.clone() {
                    let key = keys.keys().next().cloned().context("Invalid Expression")?;
                    let operator =
                        string_to_operator(key.to_owned()).context("Operator does not match")?;
                    let scalar_value = pair_value
                        .get(key.to_owned())
                        .context(format!("scalar value by key {:?} is empty", key))?;
                    let expression = value_to_simple_expression(
                        pair_key.to_owned(),
                        operator,
                        scalar_value.to_owned(),
                    )?;
                    result.push(expression);
                    continue;
                } else {
                    // Equal to operation
                    let expression = value_to_simple_expression(
                        pair_key.to_owned(),
                        Operator::EQ,
                        pair_value.to_owned(),
                    )?;
                    result.push(expression);
                }
            }
            Ok(result)
        }
        Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Null => {
            bail!("Invalid Expression")
        }
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_simple_parse_query() -> anyhow::Result<()> {
        use super::value_to_expression;
        use crate::cache::expression::FilterExpression;
        use crate::cache::expression::Operator;
        use crate::test_parse_query;
        use dozer_types::serde_json::json;
        use dozer_types::types::Field;

        test_parse_query!(
            json!({"a":  1}),
            FilterExpression::Simple("a".to_string(), Operator::EQ, Field::Int(1))
        );

        test_parse_query!(
            json!({"a":  {"$eq": 1}}),
            FilterExpression::Simple("a".to_string(), Operator::EQ, Field::Int(1))
        );

        test_parse_query!(
            json!({"a":  {"$gt": 1}}),
            FilterExpression::Simple("a".to_string(), Operator::GT, Field::Int(1))
        );

        test_parse_query!(
            json!({"a":  {"$lt": 1}}),
            FilterExpression::Simple("a".to_string(), Operator::LT, Field::Int(1))
        );

        Ok(())
    }
    #[macro_export]
    macro_rules! test_parse_query {
        ($a:expr,$b:expr) => {
            assert_eq!(value_to_expression($a)?, vec![$b], "must be equal");
        };
    }
}
