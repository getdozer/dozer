use crate::cache::{
    expression::{self, FilterExpression, QueryExpression},
    index,
    lmdb::CacheOptions,
    test_utils, Cache,
};
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

fn _setup_empty_primary_index() -> (LmdbCache, Schema, Vec<IndexDefinition>) {
    let (schema, secondary_indexes) = test_utils::schema_empty_primary_index();
    let cache = LmdbCache::new(CacheOptions::default()).unwrap();
    (cache, schema, secondary_indexes)
}

fn query_and_test(
    cache: &LmdbCache,
    inserted_record: &Record,
    schema_name: &str,
    exp: &QueryExpression,
) {
    let records = cache.query(schema_name, exp).unwrap();
    assert_eq!(records[0], inserted_record.clone(), "must be equal");
}

#[test]
fn insert_and_get_schema() {
    let (cache, schema, secondary_indexes) = _setup();
    cache
        .insert_schema("test", &schema, &secondary_indexes)
        .unwrap();
    let schema = cache.get_schema_and_indexes_by_name("test").unwrap().0;

    let get_schema = cache
        .get_schema(&schema.identifier.to_owned().unwrap())
        .unwrap();
    assert_eq!(get_schema, schema, "must be equal");
}

#[test]
fn insert_get_and_delete_record() {
    let val = "bar".to_string();
    let (cache, schema, secondary_indexes) = _setup();
    let record = Record::new(schema.identifier, vec![Field::String(val.clone())]);
    cache
        .insert_schema("docs", &schema, &secondary_indexes)
        .unwrap();
    cache.insert(&record).unwrap();

    let key = index::get_primary_key(&[0], &[Field::String(val)]);

    let get_record = cache.get(&key).unwrap();
    assert_eq!(get_record, record, "must be equal");

    cache.delete(&key).unwrap();

    cache.get(&key).expect_err("Must not find a record");
}

#[test]
fn insert_and_update_record() {
    let (cache, schema, secondary_indexes) = _setup();
    let foo = Record::new(schema.identifier, vec![Field::String("foo".to_string())]);
    let bar = Record::new(schema.identifier, vec![Field::String("bar".to_string())]);
    cache
        .insert_schema("test", &schema, &secondary_indexes)
        .unwrap();
    cache.insert(&foo).unwrap();
    cache.insert(&bar).unwrap();

    let key = index::get_primary_key(&schema.primary_index, &foo.values);

    cache.update(&key, &foo).unwrap();
}

fn insert_and_query_record_impl(
    cache: LmdbCache,
    schema: Schema,
    secondary_indexes: Vec<IndexDefinition>,
) {
    let val = "bar".to_string();
    let record = Record::new(schema.identifier, vec![Field::String(val)]);

    cache
        .insert_schema("docs", &schema, &secondary_indexes)
        .unwrap();
    cache.insert(&record).unwrap();

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

    query_and_test(&cache, &record, "docs", &exp);

    // Query without an expression
    query_and_test(
        &cache,
        &record,
        "docs",
        &QueryExpression::new(None, vec![], 10, 0),
    );
}

#[test]
fn insert_and_query_record() {
    let (cache, schema, secondary_indexes) = _setup();
    insert_and_query_record_impl(cache, schema, secondary_indexes);
    let (cache, schema, secondary_indexes) = _setup_empty_primary_index();
    insert_and_query_record_impl(cache, schema, secondary_indexes);
}
