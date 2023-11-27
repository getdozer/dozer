use dozer_types::json_types::from_value;
use dozer_types::json_types::{json, JsonValue};

use crate::cache::expression::FilterExpression;
use crate::cache::expression::Operator;
use crate::cache::expression::Skip;
use crate::cache::expression::SortOptions;
use crate::cache::expression::{
    QueryExpression,
    SortDirection::{Ascending, Descending},
    SortOption,
};

#[test]
fn test_operators() {
    let operators = vec![
        (Operator::GT, "$gt"),
        (Operator::GTE, "$gte"),
        (Operator::LT, "$lt"),
        (Operator::LTE, "$lte"),
        (Operator::EQ, "$eq"),
        (Operator::Contains, "$contains"),
        (Operator::MatchesAny, "$matches_any"),
        (Operator::MatchesAll, "$matches_all"),
    ];
    for (op, op_str) in operators {
        let fetched = from_value(&JsonValue::from(op_str.to_string())).unwrap();

        assert_eq!(op, fetched, "are equal");
    }
}

#[test]
fn test_filter_query_deserialize_simple() {
    test_deserialize_filter(
        json!({"a":  1}),
        FilterExpression::Simple("a".to_string(), Operator::EQ, JsonValue::from(1)),
    );
    test_deserialize_filter(
        json!({"ab_c":  1}),
        FilterExpression::Simple("ab_c".to_string(), Operator::EQ, JsonValue::from(1)),
    );

    test_deserialize_filter(
        json!({"a":  {"$eq": 1}}),
        FilterExpression::Simple("a".to_string(), Operator::EQ, JsonValue::from(1)),
    );

    test_deserialize_filter(
        json!({"a":  {"$gt": 1}}),
        FilterExpression::Simple("a".to_string(), Operator::GT, JsonValue::from(1)),
    );

    test_deserialize_filter(
        json!({"a":  {"$lt": 1}}),
        FilterExpression::Simple("a".to_string(), Operator::LT, JsonValue::from(1)),
    );

    test_deserialize_filter(
        json!({"a":  {"$lte": 1}}),
        FilterExpression::Simple("a".to_string(), Operator::LTE, JsonValue::from(1)),
    );
    test_deserialize_filter(
        json!({"a":  -64}),
        FilterExpression::Simple("a".to_string(), Operator::EQ, JsonValue::from(-64)),
    );
    test_deserialize_filter(
        json!({"a":  256.0}),
        FilterExpression::Simple("a".to_string(), Operator::EQ, JsonValue::from(256.0)),
    );
    test_deserialize_filter(
        json!({"a":  -256.88393}),
        FilterExpression::Simple("a".to_string(), Operator::EQ, JsonValue::from(-256.88393)),
    );
    test_deserialize_filter(
        json!({"a":  98_222}),
        FilterExpression::Simple("a".to_string(), Operator::EQ, JsonValue::from(98222)),
    );
    test_deserialize_filter(
        json!({"a":  true}),
        FilterExpression::Simple("a".to_string(), Operator::EQ, JsonValue::from(true)),
    );
    test_deserialize_filter(
        json!({ "a": null }),
        FilterExpression::Simple("a".to_string(), Operator::EQ, JsonValue::NULL),
    );

    test_deserialize_filter_error(json!({"a":  []}));
    test_deserialize_filter_error(json!({"a":  {}}));
    test_deserialize_filter_error(json!({"a":  {"lte": 1}}));
    test_deserialize_filter_error(json!({"$lte":  {"lte": 1}}));
    test_deserialize_filter_error(json!([]));
    test_deserialize_filter_error(json!(2));
    test_deserialize_filter_error(json!(true));
    test_deserialize_filter_error(json!("abc"));
    test_deserialize_filter_error(json!(2.3));
}
#[test]
fn test_filter_query_deserialize_complex() {
    test_deserialize_filter(
        json!({"a":  {"$lt": 1}, "b":  {"$gte": 3}}),
        FilterExpression::And(vec![
            FilterExpression::Simple("a".to_string(), Operator::LT, JsonValue::from(1)),
            FilterExpression::Simple("b".to_string(), Operator::GTE, JsonValue::from(3)),
        ]),
    );
    // AND with 3 expression
    let three_fields = FilterExpression::And(vec![
        FilterExpression::Simple("a".to_string(), Operator::LT, JsonValue::from(1)),
        FilterExpression::Simple("b".to_string(), Operator::GTE, JsonValue::from(3)),
        FilterExpression::Simple("c".to_string(), Operator::EQ, JsonValue::from(3)),
    ]);
    test_deserialize_filter(
        json!({"a":  {"$lt": 1}, "b":  {"$gte": 3}, "c": 3}),
        three_fields,
    );

    // Same expression with different conditions
    test_deserialize_filter(
        json!({ "$and": [{"film_id":  {"$lt": 500}}, {"film_id":  {"$gte": 2}}]}),
        FilterExpression::And(vec![
            FilterExpression::Simple("film_id".to_string(), Operator::LT, JsonValue::from(500)),
            FilterExpression::Simple("film_id".to_string(), Operator::GTE, JsonValue::from(2)),
        ]),
    );

    test_deserialize_filter_error(json!({"$and": {}}));
    test_deserialize_filter_error(json!({"$and": [{"a":  {"lt": 1}}, {"b":  {"$gt": 1}}]}));
    test_deserialize_filter_error(json!({"and": [{"a":  {"$lt": 1}}]}));
}

#[test]
fn test_sort_options_query_deserialize() {
    test_deserialize_sort_options(json!({}), vec![]);
    test_deserialize_sort_options(
        json!({"a": "asc", "b": "desc"}),
        vec![
            SortOption::new("a".into(), Ascending),
            SortOption::new("b".into(), Descending),
        ],
    );

    test_deserialize_sort_options_error(json!(""));
    test_deserialize_sort_options_error(json!(1));
    test_deserialize_sort_options_error(json!(1.2));
    test_deserialize_sort_options_error(json!(true));
    test_deserialize_sort_options_error(json!(false));
    test_deserialize_sort_options_error(json!(null));
    test_deserialize_filter_error(json!([]));
    test_deserialize_sort_options_error(json!({"a": "string"}));
    test_deserialize_sort_options_error(json!({"a": 1}));
    test_deserialize_sort_options_error(json!({"a": 1.2}));
    test_deserialize_sort_options_error(json!({"a": true}));
    test_deserialize_sort_options_error(json!({"a": false}));
    test_deserialize_sort_options_error(json!({ "a": null }));
    test_deserialize_sort_options_error(json!({"a": []}));
    test_deserialize_sort_options_error(json!({"a": {}}));
}

#[test]
fn test_query_expression_deserialize() {
    test_deserialize_query(
        json!({}),
        QueryExpression::new(None, vec![], None, Skip::Skip(0)),
    );
    test_deserialize_query(
        json!({"$filter": {}}),
        QueryExpression::new(
            Some(FilterExpression::And(vec![])),
            vec![],
            None,
            Skip::Skip(0),
        ),
    );
    test_deserialize_query(
        json!({"$order_by": {"abc": "asc"}}),
        QueryExpression::new(
            None,
            vec![SortOption {
                field_name: "abc".to_owned(),
                direction: Ascending,
            }],
            None,
            Skip::Skip(0),
        ),
    );
    test_deserialize_query(
        json!({"$order_by": {"abc": "asc"}, "$limit": 100, "$skip": 20}),
        QueryExpression::new(
            None,
            vec![SortOption {
                field_name: "abc".to_owned(),
                direction: Ascending,
            }],
            Some(100),
            Skip::Skip(20),
        ),
    );
    test_deserialize_query(
        json!({ "$after": 30 }),
        QueryExpression::new(None, vec![], None, Skip::After(30)),
    );
    test_deserialize_query(
        json!({"$filter": {"a":  {"$lt": 1}, "b":  {"$gte": 3}, "c": 3}}),
        QueryExpression::new(
            Some(FilterExpression::And(vec![
                FilterExpression::Simple("a".to_string(), Operator::LT, JsonValue::from(1)),
                FilterExpression::Simple("b".to_string(), Operator::GTE, JsonValue::from(3)),
                FilterExpression::Simple("c".to_string(), Operator::EQ, JsonValue::from(3)),
            ])),
            vec![],
            None,
            Skip::Skip(0),
        ),
    );
}

#[test]
fn test_query_expression_deserialize_error() {
    test_deserialize_query_error(json!({ "$skip": 20, "$after": 30 }));
}

fn test_deserialize_query(a: JsonValue, b: QueryExpression) {
    let parsed_result = from_value::<QueryExpression>(&a).unwrap();
    assert_eq!(parsed_result, b, "must be equal");
}

fn test_deserialize_query_error(a: JsonValue) {
    let parsed_result = from_value::<QueryExpression>(&a);
    assert!(parsed_result.is_err());
}

fn test_deserialize_filter(a: JsonValue, b: FilterExpression) {
    let parsed_result = from_value::<FilterExpression>(&a).unwrap();
    assert_eq!(parsed_result, b, "must be equal");
}
fn test_deserialize_filter_error(a: JsonValue) {
    let parsed_result = from_value::<FilterExpression>(&a);
    assert!(parsed_result.is_err());
}

fn test_deserialize_sort_options(json: JsonValue, expected: Vec<SortOption>) {
    assert_eq!(
        from_value::<SortOptions>(&json).unwrap(),
        SortOptions(expected)
    );
}

fn test_deserialize_sort_options_error(json: JsonValue) {
    assert!(from_value::<SortOptions>(&json).is_err());
}
