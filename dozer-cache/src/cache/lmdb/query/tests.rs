use crate::cache::{
    expression::{self, FilterExpression, QueryExpression, SortDirection, SortOptions},
    lmdb::cache::LmdbCache,
    test_utils, Cache,
};
use dozer_types::types::{Field, Record};

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

    cache.insert_with_schema(&record, &schema, "sample")?;

    let filter = FilterExpression::And(
        Box::new(FilterExpression::Simple(
            "a".to_string(),
            expression::Operator::EQ,
            Field::Int(1),
        )),
        Box::new(FilterExpression::Simple(
            "b".to_string(),
            expression::Operator::EQ,
            Field::String("test".to_string()),
        )),
    );
    // Query with an expression
    let query = QueryExpression::new(Some(filter), vec![], 10, 0);

    let records = cache.query("sample", &query)?;
    assert_eq!(records.len(), 1, "must be equal");
    assert_eq!(records[0], record.clone(), "must be equal");

    Ok(())
}
