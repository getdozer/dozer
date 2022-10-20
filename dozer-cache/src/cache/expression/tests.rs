use crate::cache::expression::FilterExpression;
use crate::cache::expression::Operator;
use crate::cache::expression::{QueryExpression, SortOptions};
use crate::{test_parse_filter_query, test_parse_filter_query_error, test_parse_query_expression};
use dozer_types::serde_json;
use dozer_types::serde_json::json;
use dozer_types::types::Field;

#[test]
fn test_filter_query_deserialize_simple() -> anyhow::Result<()> {
    test_parse_filter_query!(
        json!({"a":  1}),
        FilterExpression::Simple("a".to_string(), Operator::EQ, Field::Int(1))
    );
    test_parse_filter_query!(
        json!({"ab_c":  1}),
        FilterExpression::Simple("ab_c".to_string(), Operator::EQ, Field::Int(1))
    );

    test_parse_filter_query!(
        json!({"a":  {"$eq": 1}}),
        FilterExpression::Simple("a".to_string(), Operator::EQ, Field::Int(1))
    );

    test_parse_filter_query!(
        json!({"a":  {"$gt": 1}}),
        FilterExpression::Simple("a".to_string(), Operator::GT, Field::Int(1))
    );

    test_parse_filter_query!(
        json!({"a":  {"$lt": 1}}),
        FilterExpression::Simple("a".to_string(), Operator::LT, Field::Int(1))
    );

    test_parse_filter_query!(
        json!({"a":  {"$lte": 1}}),
        FilterExpression::Simple("a".to_string(), Operator::LTE, Field::Int(1))
    );
    test_parse_filter_query!(
        json!({"a":  -64}),
        FilterExpression::Simple("a".to_string(), Operator::EQ, Field::Int(-64))
    );
    test_parse_filter_query!(
        json!({"a":  256.0}),
        FilterExpression::Simple("a".to_string(), Operator::EQ, Field::Float(256.0))
    );
    test_parse_filter_query!(
        json!({"a":  -256.88393}),
        FilterExpression::Simple("a".to_string(), Operator::EQ, Field::Float(-256.88393))
    );
    test_parse_filter_query!(
        json!({"a":  98_222}),
        FilterExpression::Simple("a".to_string(), Operator::EQ, Field::Int(98222))
    );
    test_parse_filter_query!(
        json!({"a":  true}),
        FilterExpression::Simple("a".to_string(), Operator::EQ, Field::Boolean(true))
    );
    test_parse_filter_query!(
        json!({ "a": null }),
        FilterExpression::Simple("a".to_string(), Operator::EQ, Field::Null)
    );

    // // special character
    test_parse_filter_query_error!(json!({"_":  1}));
    test_parse_filter_query_error!(json!({"'":  1}));
    test_parse_filter_query_error!(json!({"\n":  1}));
    test_parse_filter_query_error!(json!({"❤":  1}));
    test_parse_filter_query_error!(json!({"%":  1}));
    test_parse_filter_query_error!(json!({"a":  '💝'}));
    test_parse_filter_query_error!(json!({"a":  "❤"}));

    test_parse_filter_query_error!(json!({"a":  []}));
    test_parse_filter_query_error!(json!({"a":  {}}));
    test_parse_filter_query_error!(json!({"a":  {"$lte": {}}}));
    test_parse_filter_query_error!(json!({"a":  {"$lte": []}}));
    test_parse_filter_query_error!(json!({"a":  {"lte": 1}}));
    test_parse_filter_query_error!(json!({"$lte":  {"lte": 1}}));
    test_parse_filter_query_error!(json!([]));
    test_parse_filter_query_error!(json!({}));
    test_parse_filter_query_error!(json!(2));
    test_parse_filter_query_error!(json!(true));
    test_parse_filter_query_error!(json!("abc"));
    test_parse_filter_query_error!(json!(2.3));
    Ok(())
}
#[test]
fn test_filter_query_deserialize_complex() -> anyhow::Result<()> {
    test_parse_filter_query!(
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
    test_parse_filter_query!(
        json!({"$and": [{"a":  {"$lt": 1}}, {"b":  {"$gte": 3}}, {"c": 3}]}),
        same_result_with_different_json
    );
    test_parse_filter_query!(
        json!({"$and": [{"$and":[{"a": {"$lt": 1}}, {"b":{"$gte": 3}}]}, {"c": 3}]}),
        same_result_with_different_json
    );
    test_parse_filter_query_error!(json!({"$and": [{"a":  {"$lt": 1}}]}));
    test_parse_filter_query_error!(json!({"$and": []}));
    test_parse_filter_query_error!(json!({"$and": {}}));
    test_parse_filter_query_error!(json!({"$and": [{"a":  {"lt": 1}}, {"b":  {"$gt": 1}}]}));
    test_parse_filter_query_error!(json!({"$and": [{"a":  {"$lt": 1}}, {"b":  {"$gte": {}}}]}));
    test_parse_filter_query_error!(json!({"$and": [{"$and":[{"a": 1}]}, {"c": 3}]}));
    test_parse_filter_query_error!(json!({"and": [{"a":  {"$lt": 1}}]}));
    Ok(())
}

#[test]
fn test_query_expression_deserialize() -> anyhow::Result<()> {
    test_parse_query_expression!(json!({}), QueryExpression::new(None, vec![], 50, 0));
    test_parse_query_expression!(
        json!({"$order_by": [{"field_name": "abc", "direction": "asc"}]}),
        QueryExpression::new(
            None,
            vec![SortOptions {
                field_name: "abc".to_owned(),
                direction: crate::cache::expression::SortDirection::Ascending
            }],
            50,
            0
        )
    );
    test_parse_query_expression!(
        json!({"$order_by": [{"field_name": "abc", "direction": "asc"}], "$limit": 100, "$skip": 20}),
        QueryExpression::new(
            None,
            vec![SortOptions {
                field_name: "abc".to_owned(),
                direction: crate::cache::expression::SortDirection::Ascending
            }],
            100,
            20
        )
    );
    test_parse_query_expression!(
        json!({"$filter": {"$and": [{"a":  {"$lt": 1}}, {"b":  {"$gte": 3}}, {"c": 3}]}}),
        QueryExpression::new(
            Some(FilterExpression::And(
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
            )),
            vec![],
            50,
            0
        )
    );
    Ok(())
}

#[macro_export]
macro_rules! test_parse_query_expression {
    ($a:expr,$b:expr) => {
        let parsed_result = serde_json::from_value::<QueryExpression>($a)?;
        assert_eq!(parsed_result, $b, "must be equal");
    };
}
#[macro_export]
macro_rules! test_parse_filter_query {
    ($a:expr,$b:expr) => {
        let parsed_result = serde_json::from_value::<FilterExpression>($a)?;
        assert_eq!(parsed_result, $b, "must be equal");
    };
}
#[macro_export]
macro_rules! test_parse_filter_query_error {
    ($a:expr) => {
        let parsed_result = serde_json::from_value::<FilterExpression>($a);
        assert!(parsed_result.is_err());
    };
}
