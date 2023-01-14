use crate::cache::expression::FilterExpression;
use crate::cache::expression::Operator;
use crate::cache::expression::SortOptions;
use crate::cache::expression::{
    QueryExpression,
    SortDirection::{Ascending, Descending},
    SortOption,
};
use crate::errors::CacheError;
use dozer_types::serde_json;
use dozer_types::serde_json::json;
use dozer_types::serde_json::Value;

#[test]
fn test_operators() -> Result<(), CacheError> {
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
        let fetched = Operator::convert_str(op_str).unwrap();

        assert_eq!(op, fetched, "are equal");
    }
    Ok(())
}

#[test]
fn test_filter_query_deserialize_simple() {
    test_deserialize_filter(
        json!({"a":  1}),
        FilterExpression::Simple("a".to_string(), Operator::EQ, Value::from(1)),
    );
    test_deserialize_filter(
        json!({"ab_c":  1}),
        FilterExpression::Simple("ab_c".to_string(), Operator::EQ, Value::from(1)),
    );

    test_deserialize_filter(
        json!({"a":  {"$eq": 1}}),
        FilterExpression::Simple("a".to_string(), Operator::EQ, Value::from(1)),
    );

    test_deserialize_filter(
        json!({"a":  {"$gt": 1}}),
        FilterExpression::Simple("a".to_string(), Operator::GT, Value::from(1)),
    );

    test_deserialize_filter(
        json!({"a":  {"$lt": 1}}),
        FilterExpression::Simple("a".to_string(), Operator::LT, Value::from(1)),
    );

    test_deserialize_filter(
        json!({"a":  {"$lte": 1}}),
        FilterExpression::Simple("a".to_string(), Operator::LTE, Value::from(1)),
    );
    test_deserialize_filter(
        json!({"a":  -64}),
        FilterExpression::Simple("a".to_string(), Operator::EQ, Value::from(-64)),
    );
    test_deserialize_filter(
        json!({"a":  256.0}),
        FilterExpression::Simple("a".to_string(), Operator::EQ, Value::from(256.0)),
    );
    test_deserialize_filter(
        json!({"a":  -256.88393}),
        FilterExpression::Simple("a".to_string(), Operator::EQ, Value::from(-256.88393)),
    );
    test_deserialize_filter(
        json!({"a":  98_222}),
        FilterExpression::Simple("a".to_string(), Operator::EQ, Value::from(98222)),
    );
    test_deserialize_filter(
        json!({"a":  true}),
        FilterExpression::Simple("a".to_string(), Operator::EQ, Value::from(true)),
    );
    test_deserialize_filter(
        json!({ "a": null }),
        FilterExpression::Simple("a".to_string(), Operator::EQ, Value::Null),
    );

    // // special character
    test_deserialize_filter_error(json!({"_":  1}));
    test_deserialize_filter_error(json!({"'":  1}));
    test_deserialize_filter_error(json!({"\n":  1}));
    test_deserialize_filter_error(json!({"❤":  1}));
    test_deserialize_filter_error(json!({"%":  1}));
    test_deserialize_filter_error(json!({"a":  '💝'}));
    test_deserialize_filter_error(json!({"a":  "❤"}));

    test_deserialize_filter_error(json!({"a":  []}));
    test_deserialize_filter_error(json!({"a":  {}}));
    test_deserialize_filter_error(json!({"a":  {"$lte": {}}}));
    test_deserialize_filter_error(json!({"a":  {"$lte": []}}));
    test_deserialize_filter_error(json!({"a":  {"lte": 1}}));
    test_deserialize_filter_error(json!({"$lte":  {"lte": 1}}));
    test_deserialize_filter_error(json!([]));
    test_deserialize_filter_error(json!({}));
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
    test_deserialize_filter(
        json!({"a":  {"$lt": 1}, "b":  {"$gte": 3}, "c": 3}),
        three_fields,
    );

    // Same expression with different conditions
    test_deserialize_filter(
        json!({ "$and": [{"film_id":  {"$lt": 500}}, {"film_id":  {"$gte": 2}}]}),
        FilterExpression::And(vec![
            FilterExpression::Simple("film_id".to_string(), Operator::LT, Value::from(500)),
            FilterExpression::Simple("film_id".to_string(), Operator::GTE, Value::from(2)),
        ]),
    );

    test_deserialize_filter_error(json!({"$and": [{"a":  {"$lt": 1}}]}));
    test_deserialize_filter_error(json!({"$and": []}));
    test_deserialize_filter_error(json!({"$and": {}}));
    test_deserialize_filter_error(json!({"$and": [{"a":  {"lt": 1}}, {"b":  {"$gt": 1}}]}));
    test_deserialize_filter_error(json!({"$and": [{"a":  {"$lt": 1}}, {"b":  {"$gte": {}}}]}));
    test_deserialize_filter_error(json!({"$and": [{"$and":[{"a": 1}]}, {"c": 3}]}));
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
    test_deserialize_sort_options_error(json!({"-": "asc"}));
}

#[test]
fn test_query_expression_deserialize() {
    test_deserialize_query(json!({}), QueryExpression::new(None, vec![], 50, 0));
    test_deserialize_query(
        json!({"$filter": {}}),
        QueryExpression::new(Some(FilterExpression::And(vec![])), vec![], 50, 0),
    );
    test_deserialize_query(
        json!({"$order_by": {"abc": "asc"}}),
        QueryExpression::new(
            None,
            vec![SortOption {
                field_name: "abc".to_owned(),
                direction: Ascending,
            }],
            50,
            0,
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
            100,
            20,
        ),
    );
    test_deserialize_query(
        json!({"$filter": {"a":  {"$lt": 1}, "b":  {"$gte": 3}, "c": 3}}),
        QueryExpression::new(
            Some(FilterExpression::And(vec![
                FilterExpression::Simple("a".to_string(), Operator::LT, Value::from(1)),
                FilterExpression::Simple("b".to_string(), Operator::GTE, Value::from(3)),
                FilterExpression::Simple("c".to_string(), Operator::EQ, Value::from(3)),
            ])),
            vec![],
            50,
            0,
        ),
    );
}

fn test_deserialize_query(a: Value, b: QueryExpression) {
    let parsed_result = serde_json::from_value::<QueryExpression>(a).unwrap();
    assert_eq!(parsed_result, b, "must be equal");
}

fn test_deserialize_filter(a: Value, b: FilterExpression) {
    let parsed_result = serde_json::from_value::<FilterExpression>(a).unwrap();
    assert_eq!(parsed_result, b, "must be equal");
}
fn test_deserialize_filter_error(a: Value) {
    let parsed_result = serde_json::from_value::<FilterExpression>(a);

    assert!(parsed_result.is_err());
}

fn test_deserialize_sort_options(json: Value, expected: Vec<SortOption>) {
    assert_eq!(
        serde_json::from_value::<SortOptions>(json).unwrap(),
        SortOptions(expected)
    );
}

fn test_deserialize_sort_options_error(json: Value) {
    assert!(serde_json::from_value::<SortOptions>(json).is_err());
}
