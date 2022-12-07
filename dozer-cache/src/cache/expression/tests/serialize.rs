use crate::cache::expression::FilterExpression;
use crate::cache::expression::Operator;
use crate::cache::expression::SortOption;
use crate::cache::expression::SortOptions;
use dozer_types::serde_json;
use dozer_types::serde_json::json;
use dozer_types::serde_json::Value;
use dozer_types::types::SortDirection::{Ascending, Descending};

#[test]
fn test_serialize_filter_simple() {
    test_serialize_filter(
        json!({"a":  1}),
        FilterExpression::Simple("a".to_string(), Operator::EQ, Value::from(1)),
    );
    test_serialize_filter(
        json!({"ab_c":  1}),
        FilterExpression::Simple("ab_c".to_string(), Operator::EQ, Value::from(1)),
    );

    test_serialize_filter(
        json!({"a":  {"$gt": 1}}),
        FilterExpression::Simple("a".to_string(), Operator::GT, Value::from(1)),
    );

    test_serialize_filter(
        json!({"a":  {"$lt": 1}}),
        FilterExpression::Simple("a".to_string(), Operator::LT, Value::from(1)),
    );

    test_serialize_filter(
        json!({"a":  {"$lte": 1}}),
        FilterExpression::Simple("a".to_string(), Operator::LTE, Value::from(1)),
    );
    test_serialize_filter(
        json!({"a":  -64}),
        FilterExpression::Simple("a".to_string(), Operator::EQ, Value::from(-64)),
    );
    test_serialize_filter(
        json!({"a":  256.0}),
        FilterExpression::Simple("a".to_string(), Operator::EQ, Value::from(256.0)),
    );
    test_serialize_filter(
        json!({"a":  -256.88393}),
        FilterExpression::Simple("a".to_string(), Operator::EQ, Value::from(-256.88393)),
    );
    test_serialize_filter(
        json!({"a":  98_222}),
        FilterExpression::Simple("a".to_string(), Operator::EQ, Value::from(98222)),
    );
    test_serialize_filter(
        json!({"a":  true}),
        FilterExpression::Simple("a".to_string(), Operator::EQ, Value::from(true)),
    );
    test_serialize_filter(
        json!({ "a": null }),
        FilterExpression::Simple("a".to_string(), Operator::EQ, Value::Null),
    );
}
#[test]
fn test_serialize_filter_complex() {
    test_serialize_filter(
        json!({"$and": [{"a":  {"$lt": 1}}, {"b":  {"$gte": 3}}]}),
        FilterExpression::And(vec![
            FilterExpression::Simple("a".to_string(), Operator::LT, Value::from(1)),
            FilterExpression::Simple("b".to_string(), Operator::GTE, Value::from(3)),
        ]),
    );
    // AND with 3 expression
    let three_fields = FilterExpression::And(vec![
        FilterExpression::Simple("a".to_string(), Operator::LT, Value::from(1)),
        FilterExpression::Simple("b".to_string(), Operator::GTE, Value::from(3)),
        FilterExpression::Simple("c".to_string(), Operator::EQ, Value::from(3)),
    ]);
    test_serialize_filter(
        json!({"$and":[{"a":  {"$lt": 1}}, {"b":  {"$gte": 3}}, {"c": 3}]}),
        three_fields,
    );
}

#[test]
fn test_serialize_sort_options() {
    test_serialize_sort_options_impl(vec![], json!({}));
    test_serialize_sort_options_impl(
        vec![SortOption::new("a".into(), Ascending)],
        json!({"a": "asc"}),
    );
    test_serialize_sort_options_impl(
        vec![SortOption::new("b".into(), Descending)],
        json!({"b": "desc"}),
    );
    test_serialize_sort_options_impl(
        vec![
            SortOption::new("a".into(), Ascending),
            SortOption::new("b".into(), Descending),
        ],
        json!({"a": "asc", "b": "desc"}),
    );
    test_serialize_sort_options_impl(
        vec![
            SortOption::new("b".into(), Ascending),
            SortOption::new("a".into(), Descending),
        ],
        json!({"b": "asc", "a": "desc"}),
    );
}

fn test_serialize_filter(a: Value, b: FilterExpression) {
    let serialized = serde_json::to_value(b).unwrap();
    assert_eq!(a, serialized, "must be equal");
}

fn test_serialize_sort_options_impl(sort_options: Vec<SortOption>, json: Value) {
    assert_eq!(
        serde_json::to_value(SortOptions(sort_options)).unwrap(),
        json,
    );
}
