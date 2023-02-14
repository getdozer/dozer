use crate::cache::{
    expression::{self, FilterExpression, QueryExpression},
    index, test_utils, RoCache, RwCache,
};
use dozer_types::{
    serde_json::Value,
    types::{Field, IndexDefinition, Record, Schema},
};

use super::super::cache::LmdbRwCache;

fn _setup() -> (LmdbRwCache, Schema, Vec<IndexDefinition>) {
    let (schema, secondary_indexes) = test_utils::schema_0();
    let cache = LmdbRwCache::new(Default::default(), Default::default()).unwrap();
    (cache, schema, secondary_indexes)
}

fn _setup_empty_primary_index() -> (LmdbRwCache, Schema, Vec<IndexDefinition>) {
    let (schema, secondary_indexes) = test_utils::schema_empty_primary_index();
    let cache = LmdbRwCache::new(Default::default(), Default::default()).unwrap();
    (cache, schema, secondary_indexes)
}

fn query_and_test(
    cache: &LmdbRwCache,
    inserted_record: &Record,
    schema_name: &str,
    exp: &QueryExpression,
) {
    let records = cache.query(schema_name, exp).unwrap();
    assert_eq!(records[0].record, inserted_record.clone(), "must be equal");
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
    let mut record = Record::new(schema.identifier, vec![Field::String(val.clone())], None);
    cache
        .insert_schema("docs", &schema, &secondary_indexes)
        .unwrap();
    cache.insert(&mut record).unwrap();
    let version = record.version.unwrap();

    let key = index::get_primary_key(&[0], &[Field::String(val)]);

    let get_record = cache.get(&key).unwrap();
    assert_eq!(get_record, record, "must be equal");

    assert_eq!(cache.delete(&key).unwrap(), version);

    cache.get(&key).expect_err("Must not find a record");
}

#[test]
fn insert_and_update_record() {
    let (cache, schema, secondary_indexes) = _setup();
    let mut foo = Record::new(
        schema.identifier,
        vec![Field::String("foo".to_string())],
        None,
    );
    let mut bar = Record::new(
        schema.identifier,
        vec![Field::String("bar".to_string())],
        None,
    );
    cache
        .insert_schema("test", &schema, &secondary_indexes)
        .unwrap();
    cache.insert(&mut foo).unwrap();
    let old_version = foo.version.unwrap();
    cache.insert(&mut bar).unwrap();

    let key = index::get_primary_key(&schema.primary_index, &foo.values);

    assert_eq!(cache.update(&key, &mut foo).unwrap(), old_version);
    assert_eq!(foo.version.unwrap(), old_version + 1);
}

fn insert_and_query_record_impl(
    cache: LmdbRwCache,
    schema: Schema,
    secondary_indexes: Vec<IndexDefinition>,
) {
    let val = "bar".to_string();
    let mut record = Record::new(schema.identifier, vec![Field::String(val)], None);

    cache
        .insert_schema("docs", &schema, &secondary_indexes)
        .unwrap();
    cache.insert(&mut record).unwrap();

    // Query with an expression
    let exp = QueryExpression::new(
        Some(FilterExpression::Simple(
            "foo".to_string(),
            expression::Operator::EQ,
            Value::from("bar".to_string()),
        )),
        vec![],
        Some(10),
        0,
    );

    query_and_test(&cache, &record, "docs", &exp);

    // Query without an expression
    query_and_test(
        &cache,
        &record,
        "docs",
        &QueryExpression::new(None, vec![], Some(10), 0),
    );
}

#[test]
fn insert_and_query_record() {
    let (cache, schema, secondary_indexes) = _setup();
    insert_and_query_record_impl(cache, schema, secondary_indexes);
    let (cache, schema, secondary_indexes) = _setup_empty_primary_index();
    insert_and_query_record_impl(cache, schema, secondary_indexes);
}
