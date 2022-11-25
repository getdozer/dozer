use crate::cache::{
    expression::{self, FilterExpression, QueryExpression},
    index,
    lmdb::CacheOptions,
    test_utils, Cache,
};
use crate::errors::CacheError;
use dozer_types::{
    serde_json::Value,
    types::{Field, IndexDefinition, Record, Schema},
};

use super::super::cache::LmdbCache;

fn _setup() -> (LmdbCache, Schema, Vec<IndexDefinition>) {
    let (schema, secondary_indexes) = test_utils::schema_0();
    let cache = LmdbCache::new(CacheOptions::default()).unwrap();
    (cache, schema, secondary_indexes)
}

fn query_and_test(
    cache: &LmdbCache,
    inserted_record: &Record,
    schema_name: &str,
    exp: &QueryExpression,
) -> Result<(), CacheError> {
    let records = cache.query(schema_name, exp)?;
    assert_eq!(records[0], inserted_record.clone(), "must be equal");
    Ok(())
}

#[test]
fn insert_and_get_schema() -> Result<(), CacheError> {
    let (cache, schema, secondary_indexes) = _setup();
    cache.insert_schema("test", &schema, &secondary_indexes)?;
    let schema = cache.get_schema_and_indexes_by_name("test")?.0;

    let get_schema = cache.get_schema(&schema.identifier.to_owned().unwrap())?;
    assert_eq!(get_schema, schema, "must be equal");
    Ok(())
}

#[test]
fn insert_get_and_delete_record() -> Result<(), CacheError> {
    let val = "bar".to_string();
    let (cache, schema, secondary_indexes) = _setup();
    let record = Record::new(schema.identifier.clone(), vec![Field::String(val.clone())]);
    cache.insert_schema("docs", &schema, &secondary_indexes)?;
    cache.insert(&record)?;

    let key = index::get_primary_key(&[0], &[Field::String(val)]);

    let get_record = cache.get(&key)?;
    assert_eq!(get_record, record, "must be equal");

    cache.delete(&key)?;

    cache.get(&key).expect_err("Must not find a record");

    Ok(())
}

#[test]
fn insert_and_query_record() -> Result<(), CacheError> {
    let val = "bar".to_string();
    let (cache, schema, secondary_indexes) = _setup();
    let record = Record::new(schema.identifier.clone(), vec![Field::String(val)]);

    cache.insert_schema("docs", &schema, &secondary_indexes)?;
    cache.insert(&record)?;

    // Query with an expression
    let exp = QueryExpression::new(
        Some(FilterExpression::Simple(
            "foo".to_string(),
            expression::Operator::EQ,
            Value::from("bar".to_string()),
        )),
        vec![],
        10,
        0,
    );

    query_and_test(&cache, &record, "docs", &exp)?;

    // Query without an expression
    query_and_test(
        &cache,
        &record,
        "docs",
        &QueryExpression::new(None, vec![], 10, 0),
    )?;

    Ok(())
}
