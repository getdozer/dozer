use crate::cache::{
    expression::{self, FilterExpression, QueryExpression},
    index,
    lmdb::CacheOptions,
    test_utils, Cache,
};
use crate::errors::CacheError;
use dozer_types::{
    serde_json::Value,
    types::{Field, Record, Schema},
};

use super::super::cache::LmdbCache;

fn _setup() -> (LmdbCache, Schema) {
    let schema = test_utils::schema_0();
    let cache = LmdbCache::new(CacheOptions::default()).unwrap();
    (cache, schema)
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
    let (cache, schema) = _setup();
    cache.insert_schema("test", &schema)?;
    let schema = cache.get_schema_by_name("test")?;

    let get_schema = cache.get_schema(&schema.identifier.to_owned().unwrap())?;
    assert_eq!(get_schema, schema, "must be equal");
    Ok(())
}

#[test]
fn insert_get_and_delete_record() {
    let val = "bar".to_string();
    let (cache, schema) = _setup();
    let record = Record::new(schema.identifier.clone(), vec![Field::String(val.clone())]);
    cache.insert_schema("docs", &schema).unwrap();
    cache.insert(&record).unwrap();

    let key = index::get_primary_key(&[0], &[Field::String(val)]).unwrap();

    let get_record = cache.get(&key).unwrap();
    assert_eq!(get_record, record, "must be equal");

    cache.delete(&key).unwrap();

    cache.get(&key).expect_err("Must not find a record");
}

#[test]
fn insert_and_query_record() -> Result<(), CacheError> {
    let val = "bar".to_string();
    let (cache, schema) = _setup();
    let record = Record::new(
        schema.identifier.clone(),
        vec![Field::Int(0), Field::String(val)],
    );

    cache.insert_schema("docs", &schema)?;
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
