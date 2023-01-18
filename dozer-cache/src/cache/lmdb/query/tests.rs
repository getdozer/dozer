use crate::cache::{
    expression::{self, FilterExpression, QueryExpression},
    lmdb::{cache::LmdbCache, tests::utils, CacheOptions},
    test_utils, Cache,
};
use dozer_types::{
    serde_json::{self, json, Value},
    types::{Field, Record, Schema},
};

#[test]
fn query_secondary() {
    let cache = LmdbCache::new(CacheOptions::default()).unwrap();
    let (schema, seconary_indexes) = test_utils::schema_1();
    let record = Record::new(
        schema.identifier,
        vec![
            Field::Int(1),
            Field::String("test".to_string()),
            Field::Int(2),
        ],
        None,
    );

    cache
        .insert_schema("sample", &schema, &seconary_indexes)
        .unwrap();
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
    let query = QueryExpression::new(Some(filter), vec![], Some(10), 0);

    let records = cache.query("sample", &query).unwrap();
    assert_eq!(cache.count("sample", &query).unwrap(), 1);
    assert_eq!(records.len(), 1, "must be equal");
    assert_eq!(records[0], record, "must be equal");

    // Full text query.
    let (schema, secondary_indexes) = test_utils::schema_full_text();
    let record = Record::new(
        schema.identifier,
        vec![
            Field::String("today is a good day".into()),
            Field::Text("marry has a little lamb".into()),
        ],
        None,
    );

    cache
        .insert_schema("full_text_sample", &schema, &secondary_indexes)
        .unwrap();
    cache.insert(&record).unwrap();

    let filter =
        FilterExpression::Simple("foo".into(), expression::Operator::Contains, "good".into());

    let query = QueryExpression::new(Some(filter), vec![], Some(10), 0);

    let records = cache.query("full_text_sample", &query).unwrap();
    assert_eq!(cache.count("full_text_sample", &query).unwrap(), 1);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0], record);

    let filter =
        FilterExpression::Simple("bar".into(), expression::Operator::Contains, "lamb".into());
    let query = QueryExpression::new(Some(filter), vec![], Some(10), 0);
    let records = cache.query("full_text_sample", &query).unwrap();
    assert_eq!(cache.count("full_text_sample", &query).unwrap(), 1);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0], record);
}

#[test]
fn query_secondary_vars() {
    let cache = LmdbCache::new(CacheOptions::default()).unwrap();
    let (schema, seconary_indexes) = test_utils::schema_1();

    cache
        .insert_schema("sample", &schema, &seconary_indexes)
        .unwrap();

    let items = vec![
        (1, Some("yuri".to_string()), Some(521)),
        (2, Some("mega".to_string()), Some(521)),
        (3, Some("james".to_string()), Some(523)),
        (4, Some("james".to_string()), Some(524)),
        (5, Some("steff".to_string()), Some(526)),
        (6, Some("mega".to_string()), Some(527)),
        (7, Some("james".to_string()), Some(528)),
        (8, Some("ava".to_string()), None),
    ];
    // 26 alphabets
    for val in items {
        utils::insert_rec_1(&cache, &schema, val);
    }

    test_query(json!({}), 8, &cache);

    test_query(
        json!({
            "$order_by": { "c": "desc" }
        }),
        8,
        &cache,
    );

    test_query(json!({"$filter":{ "a": {"$eq": 1}}}), 1, &cache);

    test_query(json!({"$filter":{ "a": {"$eq": null}}}), 0, &cache);

    test_query(json!({"$filter":{ "c": {"$eq": 521}}}), 2, &cache);

    test_query(json!({"$filter":{ "c": {"$eq": null}}}), 1, &cache);

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
            "$order_by": { "c": "asc" }
        }),
        2,
        &cache,
    );

    test_query_record(
        json!({
            "$filter":{ "a": {"$eq": 1}},
            "$order_by": { "b": "asc" }
        }),
        vec![(1, "yuri".to_string(), 521)],
        &schema,
        &cache,
    );

    // Range tests
    test_query(json!({"$filter":{ "c": {"$lte": null}}}), 0, &cache);

    test_query(json!({"$filter":{ "c": {"$lte": 521}}}), 2, &cache);

    test_query(json!({"$filter":{ "c": {"$gte": 521}}}), 7, &cache);

    test_query(json!({"$filter":{ "c": {"$gt": 521}}}), 5, &cache);

    test_query(json!({"$filter":{ "c": {"$lte": 524}}}), 4, &cache);

    test_query(json!({"$filter":{ "c": {"$lt": 524}}}), 3, &cache);

    test_query(json!({"$filter":{ "c": {"$lt": 600}}}), 7, &cache);

    test_query(json!({"$filter":{ "c": {"$gt": 200}}}), 7, &cache);

    test_query_record(
        json!({
            "$filter":{ "c": {"$gt": 526}},
            "$order_by": { "c": "asc" }
        }),
        vec![(6, "mega".to_string(), 527), (7, "james".to_string(), 528)],
        &schema,
        &cache,
    );

    test_query_record(
        json!({
            "$filter":{ "c": {"$gt": 526}},
            "$order_by": { "c": "desc" }
        }),
        vec![(7, "james".to_string(), 528), (6, "mega".to_string(), 527)],
        &schema,
        &cache,
    );
}

#[test]
fn query_secondary_multi_indices() {
    let cache = LmdbCache::new(CacheOptions::default()).unwrap();
    let (schema, seconary_indexes) = test_utils::schema_multi_indices();

    cache
        .insert_schema("sample", &schema, &seconary_indexes)
        .unwrap();

    for (id, text) in [
        (1, "apple ball cake dance"),
        (2, "ball cake dance egg"),
        (3, "cake dance egg fish"),
        (4, "dance egg fish glove"),
        (5, "egg fish glove heart"),
        (6, "fish glove heart igloo"),
        (7, "glove heart igloo jump"),
    ] {
        cache
            .insert(&Record {
                schema_id: schema.identifier,
                values: vec![Field::Int(id), Field::String(text.into())],
                version: None,
            })
            .unwrap();
    }

    let query = QueryExpression::new(
        Some(FilterExpression::And(vec![
            FilterExpression::Simple(
                "id".into(),
                expression::Operator::GT,
                serde_json::Value::from(2),
            ),
            FilterExpression::Simple(
                "text".into(),
                expression::Operator::Contains,
                serde_json::Value::from("dance"),
            ),
        ])),
        vec![],
        Some(10),
        0,
    );

    let records = cache.query("sample", &query).unwrap();
    assert_eq!(cache.count("sample", &query).unwrap(), 2);
    assert_eq!(
        records,
        vec![
            Record {
                schema_id: schema.identifier,
                values: vec![Field::Int(3), Field::String("cake dance egg fish".into())],
                version: None
            },
            Record {
                schema_id: schema.identifier,
                values: vec![Field::Int(4), Field::String("dance egg fish glove".into())],
                version: None
            },
        ]
    );
}

fn test_query_err(query: Value, cache: &LmdbCache) {
    let query = serde_json::from_value::<QueryExpression>(query).unwrap();
    let count_result = cache.count("sample", &query);
    let result = cache.query("sample", &query);

    assert!(matches!(
        count_result.unwrap_err(),
        crate::errors::CacheError::PlanError(_)
    ),);
    assert!(matches!(
        result.unwrap_err(),
        crate::errors::CacheError::PlanError(_)
    ),);
}
fn test_query(query: Value, count: usize, cache: &LmdbCache) {
    let query = serde_json::from_value::<QueryExpression>(query).unwrap();
    assert_eq!(cache.count("sample", &query).unwrap(), count);
    let records = cache.query("sample", &query).unwrap();

    assert_eq!(records.len(), count, "Count must be equal : {:?}", query);
}

fn test_query_record(
    query: Value,
    expected: Vec<(i64, String, i64)>,
    schema: &Schema,
    cache: &LmdbCache,
) {
    let query = serde_json::from_value::<QueryExpression>(query).unwrap();
    assert_eq!(cache.count("sample", &query).unwrap(), expected.len());
    let records = cache.query("sample", &query).unwrap();
    let expected = expected
        .into_iter()
        .map(|(a, b, c)| {
            Record::new(
                schema.identifier,
                vec![Field::Int(a), Field::String(b), Field::Int(c)],
                None,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(records, expected);
}
