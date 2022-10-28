use crate::cache::expression::FilterExpression;
use crate::cache::expression::Operator;
use crate::cache::expression::SimpleFilterExpression;
use dozer_types::serde_json;
use dozer_types::serde_json::json;
use dozer_types::serde_json::Value;

#[test]
fn test_serialize_filter_simple() {
    test_serialize_filter(
        json!({"a":  1}),
        FilterExpression::Simple(SimpleFilterExpression {
            field_name: "a".to_string(),
            operator: Operator::EQ,
            value: Value::from(1),
        }),
    );
    test_serialize_filter(
        json!({"ab_c":  1}),
        FilterExpression::Simple(SimpleFilterExpression {
            field_name: "ab_c".to_string(),
            operator: Operator::EQ,
            value: Value::from(1),
        }),
    );

    test_serialize_filter(
        json!({"a":  {"$gt": 1}}),
        FilterExpression::Simple(SimpleFilterExpression {
            field_name: "a".to_string(),
            operator: Operator::GT,
            value: Value::from(1),
        }),
    );

    test_serialize_filter(
        json!({"a":  {"$lt": 1}}),
        FilterExpression::Simple(SimpleFilterExpression {
            field_name: "a".to_string(),
            operator: Operator::LT,
            value: Value::from(1),
        }),
    );

    test_serialize_filter(
        json!({"a":  {"$lte": 1}}),
        FilterExpression::Simple(SimpleFilterExpression {
            field_name: "a".to_string(),
            operator: Operator::LTE,
            value: Value::from(1),
        }),
    );
    test_serialize_filter(
        json!({"a":  -64}),
        FilterExpression::Simple(SimpleFilterExpression {
            field_name: "a".to_string(),
            operator: Operator::EQ,
            value: Value::from(-64),
        }),
    );
    test_serialize_filter(
        json!({"a":  256.0}),
        FilterExpression::Simple(SimpleFilterExpression {
            field_name: "a".to_string(),
            operator: Operator::EQ,
            value: Value::from(256.0),
        }),
    );
    test_serialize_filter(
        json!({"a":  -256.88393}),
        FilterExpression::Simple(SimpleFilterExpression {
            field_name: "a".to_string(),
            operator: Operator::EQ,
            value: Value::from(-256.88393),
        }),
    );
    test_serialize_filter(
        json!({"a":  98_222}),
        FilterExpression::Simple(SimpleFilterExpression {
            field_name: "a".to_string(),
            operator: Operator::EQ,
            value: Value::from(98222),
        }),
    );
    test_serialize_filter(
        json!({"a":  true}),
        FilterExpression::Simple(SimpleFilterExpression {
            field_name: "a".to_string(),
            operator: Operator::EQ,
            value: Value::from(true),
        }),
    );
    test_serialize_filter(
        json!({ "a": null }),
        FilterExpression::Simple(SimpleFilterExpression {
            field_name: "a".to_string(),
            operator: Operator::EQ,
            value: Value::Null,
        }),
    );
}
#[test]
fn test_serialize_filter_complex() {
    test_serialize_filter(
        json!({"$and": [{"a":  {"$lt": 1}}, {"b":  {"$gte": 3}}]}),
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
    test_serialize_filter(
        json!({"$and":[{"a":  {"$lt": 1}}, {"b":  {"$gte": 3}}, {"c": 3}]}),
        three_fields,
    );
}

fn test_serialize_filter(a: Value, b: FilterExpression) {
    let serialized = serde_json::to_string(&b).unwrap();
    assert_eq!(a.to_string(), serialized, "must be equal");
}
