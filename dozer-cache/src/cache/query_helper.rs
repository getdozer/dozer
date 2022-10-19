use std::str::FromStr;

use super::expression::{FilterExpression, Operator};
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
            let operator: Operator = Operator::from_str(&inner_key)?;
            let scalar_value = pairs
                .get(inner_key)
                .context(format!("scalar value by key {:?} is empty", inner_key))?;
            let expression = construct_simple_expression(key, operator, scalar_value.to_owned())?;
            return Ok(expression);
        }
        Value::Number(_) | Value::String(_) | Value::Bool(_) | Value::Null => {
            let expression = construct_simple_expression(key, Operator::EQ, value.to_owned())?;
            return Ok(expression);
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
            return Ok(result);
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
#[cfg(test)]
mod tests {
    use dozer_types::serde_json;

    #[test]
    fn test_simple_parse_query() -> anyhow::Result<()> {
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
            json!({"ab_c":  1}),
            FilterExpression::Simple("ab_c".to_string(), Operator::EQ, Field::Int(1))
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

        // // special character
        test_parse_error_query!(json!({"_":  1}));
        test_parse_error_query!(json!({"'":  1}));
        test_parse_error_query!(json!({"\n":  1}));
        test_parse_error_query!(json!({"❤":  1}));
        test_parse_error_query!(json!({"%":  1}));
        test_parse_error_query!(json!({"a":  '💝'}));
        test_parse_error_query!(json!({"a":  "❤"}));

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
            let parsed_result = serde_json::from_value::<FilterExpression>($a)?;
            assert_eq!(parsed_result, $b, "must be equal");
        };
    }
    #[macro_export]
    macro_rules! test_parse_error_query {
        ($a:expr) => {
            let parsed_result = serde_json::from_value::<FilterExpression>($a);
            assert!(parsed_result.is_err());
        };
    }
}
