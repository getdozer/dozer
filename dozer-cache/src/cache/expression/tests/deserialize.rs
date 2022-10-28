use crate::cache::expression::FilterExpression;
use crate::cache::expression::Operator;
use crate::cache::expression::SimpleFilterExpression;
use crate::cache::expression::{QueryExpression, SortOptions};
use dozer_types::errors::cache::CacheError;
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
        FilterExpression::Simple(SimpleFilterExpression {
            field_name: "a".to_string(),
            operator: Operator::EQ,
            value: Value::from(1),
        }),
    );
    test_deserialize_filter(
        json!({"ab_c":  1}),
        FilterExpression::Simple(SimpleFilterExpression {
            field_name: "ab_c".to_string(),
            operator: Operator::EQ,
            value: Value::from(1),
        }),
    );

    test_deserialize_filter(
        json!({"a":  {"$eq": 1}}),
        FilterExpression::Simple(SimpleFilterExpression {
            field_name: "a".to_string(),
            operator: Operator::EQ,
            value: Value::from(1),
        }),
    );

    test_deserialize_filter(
        json!({"a":  {"$gt": 1}}),
        FilterExpression::Simple(SimpleFilterExpression {
            field_name: "a".to_string(),
            operator: Operator::GT,
            value: Value::from(1),
        }),
    );

    test_deserialize_filter(
        json!({"a":  {"$lt": 1}}),
        FilterExpression::Simple(SimpleFilterExpression {
            field_name: "a".to_string(),
            operator: Operator::LT,
            value: Value::from(1),
        }),
    );

    test_deserialize_filter(
        json!({"a":  {"$lte": 1}}),
        FilterExpression::Simple(SimpleFilterExpression {
            field_name: "a".to_string(),
            operator: Operator::LTE,
            value: Value::from(1),
        }),
    );
    test_deserialize_filter(
        json!({"a":  -64}),
        FilterExpression::Simple(SimpleFilterExpression {
            field_name: "a".to_string(),
            operator: Operator::EQ,
            value: Value::from(-64),
        }),
    );
    test_deserialize_filter(
        json!({"a":  256.0}),
        FilterExpression::Simple(SimpleFilterExpression {
            field_name: "a".to_string(),
            operator: Operator::EQ,
            value: Value::from(256.0),
        }),
    );
    test_deserialize_filter(
        json!({"a":  -256.88393}),
        FilterExpression::Simple(SimpleFilterExpression {
            field_name: "a".to_string(),
            operator: Operator::EQ,
            value: Value::from(-256.88393),
        }),
    );
    test_deserialize_filter(
        json!({"a":  98_222}),
        FilterExpression::Simple(SimpleFilterExpression {
            field_name: "a".to_string(),
            operator: Operator::EQ,
            value: Value::from(98222),
        }),
    );
    test_deserialize_filter(
        json!({"a":  true}),
        FilterExpression::Simple(SimpleFilterExpression {
            field_name: "a".to_string(),
            operator: Operator::EQ,
            value: Value::from(true),
        }),
    );
    test_deserialize_filter(
        json!({ "a": null }),
        FilterExpression::Simple(SimpleFilterExpression {
            field_name: "a".to_string(),
            operator: Operator::EQ,
            value: Value::Null,
        }),
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
            FilterExpression::Simple(SimpleFilterExpression {
                field_name: "a".to_string(),
                operator: Operator::LT,
                value: Value::from(1),
            }),
            FilterExpression::Simple(SimpleFilterExpression {
                field_name: "b".to_string(),
                operator: Operator::GTE,
                value: Value::from(3),
            }),
        ]),
    );
    // AND with 3 expression
    let three_fields = FilterExpression::And(vec![
        FilterExpression::Simple(SimpleFilterExpression {
            field_name: "a".to_string(),
            operator: Operator::LT,
            value: Value::from(1),
        }),
        FilterExpression::Simple(SimpleFilterExpression {
            field_name: "b".to_string(),
            operator: Operator::GTE,
            value: Value::from(3),
        }),
        FilterExpression::Simple(SimpleFilterExpression {
            field_name: "c".to_string(),
            operator: Operator::EQ,
            value: Value::from(3),
        }),
    ]);
    test_deserialize_filter(
        json!({"a":  {"$lt": 1}, "b":  {"$gte": 3}, "c": 3}),
        three_fields,
    );

    // Same expression with different conditions
    test_deserialize_filter(
        json!({ "$and": [{"film_id":  {"$lt": 500}}, {"film_id":  {"$gte": 2}}]}),
        FilterExpression::And(vec![
            FilterExpression::Simple(SimpleFilterExpression {
                field_name: "film_id".to_string(),
                operator: Operator::LT,
                value: Value::from(500),
            }),
            FilterExpression::Simple(SimpleFilterExpression {
                field_name: "film_id".to_string(),
                operator: Operator::GTE,
                value: Value::from(2),
            }),
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
fn test_query_expression_deserialize() {
    test_deserialize_query(json!({}), QueryExpression::new(None, vec![], 50, 0));
    test_deserialize_query(
        json!({"$order_by": [{"field_name": "abc", "direction": "asc"}]}),
        QueryExpression::new(
            None,
            vec![SortOptions {
                field_name: "abc".to_owned(),
                direction: crate::cache::expression::SortDirection::Ascending,
            }],
            50,
            0,
        ),
    );
    test_deserialize_query(
        json!({"$order_by": [{"field_name": "abc", "direction": "asc"}], "$limit": 100, "$skip": 20}),
        QueryExpression::new(
            None,
            vec![SortOptions {
                field_name: "abc".to_owned(),
                direction: crate::cache::expression::SortDirection::Ascending,
            }],
            100,
            20,
        ),
    );
    test_deserialize_query(
        json!({"$filter": {"a":  {"$lt": 1}, "b":  {"$gte": 3}, "c": 3}}),
        QueryExpression::new(
            Some(FilterExpression::And(vec![
                FilterExpression::Simple(SimpleFilterExpression {
                    field_name: "a".to_string(),
                    operator: Operator::LT,
                    value: Value::from(1),
                }),
                FilterExpression::Simple(SimpleFilterExpression {
                    field_name: "b".to_string(),
                    operator: Operator::GTE,
                    value: Value::from(3),
                }),
                FilterExpression::Simple(SimpleFilterExpression {
                    field_name: "c".to_string(),
                    operator: Operator::EQ,
                    value: Value::from(3),
                }),
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
