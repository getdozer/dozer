use super::expression::{FilterExpression, Operator};
use anyhow::{bail, ensure, Context, Ok};
use dozer_types::json_value_to_field;
use dozer_types::serde_json::Value;
use std::str::FromStr;

fn is_combinator(input: String) -> bool {
    vec!["$or", "$and"].contains(&input.to_lowercase().as_str())
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
        "$and" => {
            ensure!(
                array_condition.len() > 1,
                "AND require at least 2 conditions input"
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

pub fn value_to_expression(input: Value) -> anyhow::Result<Vec<FilterExpression>> {
    match input {
        Value::Array(array_value) => {
            ensure!(
                !array_value.is_empty(),
                "Array expression input must have some value"
            );
            let mut result: Vec<FilterExpression> = Vec::new();
            for value in array_value {
                let expression = value_to_expression(value)?;
                result.extend(expression);
            }
            Ok(result)
        }
        Value::Object(pairs) => {
            ensure!(!pairs.is_empty(), "Empty object input");
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
                    let operator = Operator::from_str(&key)?;
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
        use crate::{test_parse_error_query, test_parse_query};
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

        test_parse_query!(
            json!({"a":  {"$lte": 1}}),
            FilterExpression::Simple("a".to_string(), Operator::LTE, Field::Int(1))
        );
        test_parse_query!(
            json!({"a":  -64}),
            FilterExpression::Simple("a".to_string(), Operator::EQ, Field::Int(-64))
        );
        test_parse_query!(
            json!({"a":  256.0}),
            FilterExpression::Simple("a".to_string(), Operator::EQ, Field::Float(256.0))
        );
        test_parse_query!(
            json!({"a":  -256.88393}),
            FilterExpression::Simple("a".to_string(), Operator::EQ, Field::Float(-256.88393))
        );
        test_parse_query!(
            json!({"a":  98_222}),
            FilterExpression::Simple("a".to_string(), Operator::EQ, Field::Int(98222))
        );
        test_parse_query!(
            json!({"a":  true}),
            FilterExpression::Simple("a".to_string(), Operator::EQ, Field::Boolean(true))
        );
        test_parse_query!(
            json!({ "a": null }),
            FilterExpression::Simple("a".to_string(), Operator::EQ, Field::Null)
        );

        test_parse_error_query!(json!({"a":  []}));
        test_parse_error_query!(json!({"a":  {}}));
        test_parse_error_query!(json!({"a":  {"$lte": {}}}));
        test_parse_error_query!(json!({"a":  {"$lte": []}}));
        test_parse_error_query!(json!({"a":  {"lte": 1}}));
        test_parse_error_query!(json!({"$lte":  {"lte": 1}}));
        test_parse_error_query!(json!([]));
        test_parse_error_query!(json!({}));
        test_parse_error_query!(json!(2));
        test_parse_error_query!(json!(true));
        test_parse_error_query!(json!("abc"));
        test_parse_error_query!(json!(2.3));
        Ok(())
    }
    #[test]
    fn test_complex_parse_query() -> anyhow::Result<()> {
        use super::value_to_expression;
        use crate::cache::expression::FilterExpression;
        use crate::cache::expression::Operator;
        use crate::{test_parse_error_query, test_parse_query};
        use dozer_types::serde_json::json;
        use dozer_types::types::Field;

        test_parse_query!(
            json!({"$and": [{"a":  {"$lt": 1}}, {"b":  {"$gte": 3}}]}),
            FilterExpression::And(
                Box::new(FilterExpression::Simple(
                    "a".to_string(),
                    Operator::LT,
                    Field::Int(1)
                )),
                Box::new(FilterExpression::Simple(
                    "b".to_string(),
                    Operator::GTE,
                    Field::Int(3)
                ))
            )
        );
        // AND with 3 expression
        let same_result_with_different_json = FilterExpression::And(
            Box::new(FilterExpression::And(
                Box::new(FilterExpression::Simple(
                    "a".to_string(),
                    Operator::LT,
                    Field::Int(1),
                )),
                Box::new(FilterExpression::Simple(
                    "b".to_string(),
                    Operator::GTE,
                    Field::Int(3),
                )),
            )),
            Box::new(FilterExpression::Simple(
                "c".to_string(),
                Operator::EQ,
                Field::Int(3),
            )),
        );
        test_parse_query!(
            json!({"$and": [{"a":  {"$lt": 1}}, {"b":  {"$gte": 3}}, {"c": 3}]}),
            same_result_with_different_json.clone()
        );
        test_parse_query!(
            json!({"$and": [{"$and":[{"a": {"$lt": 1}}, {"b":{"$gte": 3}}]}, {"c": 3}]}),
            same_result_with_different_json
        );

        test_parse_error_query!(json!({"$and": [{"a":  {"$lt": 1}}]}));
        test_parse_error_query!(json!({"$and": []}));
        test_parse_error_query!(json!({"$and": {}}));
        test_parse_error_query!(json!({"$and": [{"a":  {"lt": 1}}, {"b":  {"$gt": 1}}]}));
        test_parse_error_query!(json!({"$and": [{"a":  {"$lt": 1}}, {"b":  {"$gte": {}}}]}));
        test_parse_error_query!(json!({"$and": [{"$and":[{"a": 1}]}, {"c": 3}]}));
        test_parse_error_query!(json!({"and": [{"a":  {"$lt": 1}}]}));

        Ok(())
    }
    #[macro_export]
    macro_rules! test_parse_query {
        ($a:expr,$b:expr) => {
            assert_eq!(value_to_expression($a)?, vec![$b], "must be equal");
        };
    }
    #[macro_export]
    macro_rules! test_parse_error_query {
        ($a:expr) => {
            assert!(value_to_expression($a).is_err());
        };
    }
}
