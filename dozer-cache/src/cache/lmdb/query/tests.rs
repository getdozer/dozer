use crate::cache::{
    expression::{self, FilterExpression, QueryExpression},
    lmdb::cache::LmdbCache,
    test_utils, Cache,
};
use dozer_types::{
    serde_json,
    types::{Field, Record},
};

#[test]
fn query_secondary() {
    let cache = LmdbCache::new(true, None);
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
