use crate::cache::{
    expression::{FilterExpression, Operator, QueryExpression},
    lmdb::tests::utils::{create_cache, insert_rec_1},
    test_utils::{query_from_filter, schema_1, schema_full_text, schema_multi_indices},
    CacheRecord, RoCache, RwCache,
};
use dozer_types::{
    serde_json::{from_value, json, Value},
    types::{Field, Record, Schema},
};

#[test]
fn query_secondary_sorted_inverted() {
    let (mut cache, indexing_thread_pool, schema, _) = create_cache(schema_1);

    let record = Record::new(
        schema.identifier,
        vec![
            Field::Int(1),
            Field::String("test".to_string()),
            Field::Int(2),
        ],
    );

    cache.insert(&record).unwrap();
    cache.commit().unwrap();
    indexing_thread_pool.lock().wait_until_catchup();

    let filter = FilterExpression::And(vec![
        FilterExpression::Simple("a".to_string(), Operator::EQ, Value::from(1)),
        FilterExpression::Simple(
            "b".to_string(),
            Operator::EQ,
            Value::from("test".to_string()),
        ),
    ]);

    // Query with an expression
    let query = query_from_filter(filter);

    let records = cache.query(&query).unwrap();
    assert_eq!(cache.count(&query).unwrap(), 1);
    assert_eq!(records.len(), 1, "must be equal");
    assert_eq!(records[0].record, record, "must be equal");
}

#[test]
fn query_secondary_full_text() {
    let (mut cache, indexing_thread_pool, schema, _) = create_cache(schema_full_text);

    let record = Record::new(
        schema.identifier,
        vec![
            Field::String("today is a good day".into()),
            Field::Text("marry has a little lamb".into()),
        ],
    );

    cache.insert(&record).unwrap();
    cache.commit().unwrap();
    indexing_thread_pool.lock().wait_until_catchup();

    let filter = FilterExpression::Simple("foo".into(), Operator::Contains, "good".into());

    let query = query_from_filter(filter);

    let records = cache.query(&query).unwrap();
    assert_eq!(cache.count(&query).unwrap(), 1);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].record, record);

    let filter = FilterExpression::Simple("bar".into(), Operator::Contains, "lamb".into());
    let query = query_from_filter(filter);
    let records = cache.query(&query).unwrap();
    assert_eq!(cache.count(&query).unwrap(), 1);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].record, record);
}

#[test]
fn query_secondary_vars() {
    let (mut cache, indexing_thread_pool, schema, _) = create_cache(schema_1);

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
        insert_rec_1(&mut cache, &schema, val);
    }
    cache.commit().unwrap();
    indexing_thread_pool.lock().wait_until_catchup();

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
        vec![(0, 1, "yuri".to_string(), 521)],
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
        vec![
            (5, 6, "mega".to_string(), 527),
            (6, 7, "james".to_string(), 528),
        ],
        &schema,
        &cache,
    );

    test_query_record(
        json!({
            "$filter":{ "c": {"$gt": 526}},
            "$order_by": { "c": "desc" }
        }),
        vec![
            (6, 7, "james".to_string(), 528),
            (5, 6, "mega".to_string(), 527),
        ],
        &schema,
        &cache,
    );
}

#[test]
fn query_secondary_multi_indices() {
    let (mut cache, indexing_thread_pool, schema, _) = create_cache(schema_multi_indices);

    for (id, text) in [
        (1, "apple ball cake dance"),
        (2, "ball cake dance egg"),
        (3, "cake dance egg fish"),
        (4, "dance egg fish glove"),
        (5, "egg fish glove heart"),
        (6, "fish glove heart igloo"),
        (7, "glove heart igloo jump"),
    ] {
        let record = Record {
            schema_id: schema.identifier,
            values: vec![Field::Int(id), Field::String(text.into())],
            lifetime: None,
        };
        cache.insert(&record).unwrap();
    }
    cache.commit().unwrap();
    indexing_thread_pool.lock().wait_until_catchup();

    let query = query_from_filter(FilterExpression::And(vec![
        FilterExpression::Simple("id".into(), Operator::GT, Value::from(2)),
        FilterExpression::Simple("text".into(), Operator::Contains, Value::from("dance")),
    ]));

    let records = cache.query(&query).unwrap();
    assert_eq!(cache.count(&query).unwrap(), 2);
    assert_eq!(
        records,
        vec![
            CacheRecord::new(
                2,
                1,
                Record {
                    schema_id: schema.identifier,
                    values: vec![Field::Int(3), Field::String("cake dance egg fish".into())],
                    lifetime: None,
                }
            ),
            CacheRecord::new(
                3,
                1,
                Record {
                    schema_id: schema.identifier,
                    values: vec![Field::Int(4), Field::String("dance egg fish glove".into())],
                    lifetime: None,
                }
            ),
        ]
    );
}

fn test_query_err(query: Value, cache: &dyn RwCache) {
    let query = from_value::<QueryExpression>(query).unwrap();
    let count_result = cache.count(&query);
    let result = cache.query(&query);

    assert!(matches!(
        count_result.unwrap_err(),
        crate::errors::CacheError::Plan(_)
    ),);
    assert!(matches!(
        result.unwrap_err(),
        crate::errors::CacheError::Plan(_)
    ),);
}
fn test_query(query: Value, count: usize, cache: &dyn RwCache) {
    let query = from_value::<QueryExpression>(query).unwrap();
    assert_eq!(cache.count(&query).unwrap(), count);
    let records = cache.query(&query).unwrap();

    assert_eq!(records.len(), count, "Count must be equal : {query:?}");
}

fn test_query_record(
    query: Value,
    expected: Vec<(u64, i64, String, i64)>,
    schema: &Schema,
    cache: &dyn RwCache,
) {
    let query = from_value::<QueryExpression>(query).unwrap();
    assert_eq!(cache.count(&query).unwrap(), expected.len());
    let records = cache.query(&query).unwrap();
    let expected = expected
        .into_iter()
        .map(|(id, a, b, c)| {
            CacheRecord::new(
                id,
                1,
                Record::new(
                    schema.identifier,
                    vec![Field::Int(a), Field::String(b), Field::Int(c)],
                ),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(records, expected);
}
