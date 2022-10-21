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
fn query_secondary() -> anyhow::Result<()> {
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

    cache.insert_schema("sample", &schema)?;
    cache.insert(&record)?;

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

    let records = cache.query("sample", &query)?;
    assert_eq!(records.len(), 1, "must be equal");
    assert_eq!(records[0], record, "must be equal");

    Ok(())
}
