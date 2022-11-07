use crate::cache::{
    expression::{self, FilterExpression, QueryExpression},
    lmdb::{cache::LmdbCache, test_utils::insert_rec_1},
    test_utils, Cache,
};
use dozer_types::{
    serde_json::{self, json, Value},
    types::{Field, Record},
};

#[test]
fn query_secondary() {
    let cache = LmdbCache::new(true);
    let schema = test_utils::schema_1();
    let record = Record::new(
        schema.identifier.clone(),
        vec![
            Field::Int(1),
            Field::String("test".to_string()),
            Field::Int(2),
        ],
    );

    cache.insert_schema("sample", &schema).unwrap();
    cache.insert(&record).unwrap();

    let filter = FilterExpression::And(vec![
        FilterExpression::Simple(
            "a".to_string(),
            expression::Operator::EQ,
            serde_json::Value::from(1),
        ),
        FilterExpression::Simple(
            "b".to_string(),
            expression::Operator::EQ,
            serde_json::Value::from("test".to_string()),
        ),
    ]);

    // Query with an expression
    let query = QueryExpression::new(Some(filter), vec![], 10, 0);

    let records = cache.query("sample", &query).unwrap();
    assert_eq!(records.len(), 1, "must be equal");
    assert_eq!(records[0], record, "must be equal");

    // Full text query.
    let schema = test_utils::schema_full_text_single();
    let record = Record::new(
        schema.identifier.clone(),
        vec![Field::String("today is a good day".into())],
    );

    cache.insert_schema("full_text_sample", &schema).unwrap();
    cache.insert(&record).unwrap();

    let filter = FilterExpression::Simple(
        "foo".into(),
        expression::Operator::Contains,
        "good".to_string().into(),
    );

    let query = QueryExpression::new(Some(filter), vec![], 10, 0);

    let records = cache.query("full_text_sample", &query).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0], record);
}

#[test]
fn query_secondary_vars() {
    let cache = LmdbCache::new(true);
    let schema = test_utils::schema_1();

    cache.insert_schema("sample", &schema).unwrap();

    let items: Vec<(i64, String, i64)> = vec![
        (1, "yuri".to_string(), 521),
        (2, "mega".to_string(), 521),
        (3, "james".to_string(), 523),
        (4, "james".to_string(), 524),
        (5, "steff".to_string(), 526),
        (6, "mega".to_string(), 527),
        (7, "james".to_string(), 528),
    ];
    // 26 alphabets
    for val in items {
        insert_rec_1(&cache, &schema, val);
    }

    test_query(json!({}), 7, &cache);

    test_query(json!({"$filter":{ "a": {"$eq": 1}}}), 1, &cache);

    test_query(json!({"$filter":{ "c": {"$eq": 521}}}), 2, &cache);

    test_query(
        json!({"$filter":{ "a": 1, "b": "yuri".to_string()}}),
        1,
        &cache,
    );

    // No compound index for a,c
    test_query_err(json!({"$filter":{ "a": 1, "c": 521}}), &cache);

    test_query(
        json!({
            "$filter":{ "c": {"$eq": 521}},
            "$order_by": [{"field_name": "c", "direction": "asc"}]
        }),
        2,
        &cache,
    );

    // Range tests
    test_query(json!({"$filter":{ "c": {"$lte": 521}}}), 2, &cache);

    test_query(json!({"$filter":{ "c": {"$gte": 521}}}), 7, &cache);

    test_query(json!({"$filter":{ "c": {"$gt": 521}}}), 5, &cache);

    test_query(json!({"$filter":{ "c": {"$lte": 524}}}), 4, &cache);

    test_query(json!({"$filter":{ "c": {"$lt": 524}}}), 3, &cache);

    test_query(json!({"$filter":{ "c": {"$lt": 600}}}), 7, &cache);

    test_query(json!({"$filter":{ "c": {"$gt": 200}}}), 7, &cache);
}

fn test_query_err(query: Value, cache: &LmdbCache) {
    let query = serde_json::from_value::<QueryExpression>(query).unwrap();
    let result = cache.query("sample", &query);

    assert!(result.is_err());
    if let Err(err) = result {
        assert!(
            matches!(err, crate::errors::CacheError::PlanError(_)),
            "Must be a PlanError"
        );
    }
}
fn test_query(query: Value, count: usize, cache: &LmdbCache) {
    let query = serde_json::from_value::<QueryExpression>(query).unwrap();
    let records = cache.query("sample", &query).unwrap();

    assert_eq!(records.len(), count, "Count must be equal : {:?}", query);
}
