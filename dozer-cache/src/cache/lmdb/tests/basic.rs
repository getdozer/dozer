use crate::cache::{
    expression::{self, FilterExpression, QueryExpression, Skip},
    index,
    lmdb::cache::LmdbRwCache,
    test_utils::{self, query_from_filter},
    RoCache, RwCache,
};
use dozer_types::{
    serde_json::Value,
    types::{Field, Record, Schema},
};

use super::utils::create_cache;

fn _setup() -> (LmdbRwCache, Schema, &'static str) {
    let schema_name = "doc";
    let (cache, schema, _) = create_cache(schema_name, test_utils::schema_0);
    (cache, schema, schema_name)
}

fn _setup_empty_primary_index() -> (LmdbRwCache, Schema, &'static str) {
    let schema_name = "doc";
    let (cache, schema, _) = create_cache(schema_name, test_utils::schema_empty_primary_index);
    (cache, schema, schema_name)
}

fn query_and_test(
    cache: &dyn RwCache,
    inserted_record: &Record,
    schema_name: &str,
    exp: &QueryExpression,
) {
    let records = cache.query(schema_name, exp).unwrap().1;
    assert_eq!(records[0].record, inserted_record.clone(), "must be equal");
}

#[test]
fn get_schema() {
    let (cache, _, schema_name) = _setup();
    let schema = &cache.get_schema_and_indexes_by_name(schema_name).unwrap().0;

    let get_schema = cache.get_schema(schema.identifier.unwrap()).unwrap();
    assert_eq!(get_schema, schema, "must be equal");
}

#[test]
fn insert_get_and_delete_record() {
    let val = "bar".to_string();
    let (cache, schema, _) = _setup();
    let mut record = Record::new(schema.identifier, vec![Field::String(val.clone())], None);
    cache.insert(&mut record).unwrap();
    let version = record.version.unwrap();

    let key = index::get_primary_key(&[0], &[Field::String(val)]);

    let get_record = cache.get(&key).unwrap().record;
    assert_eq!(get_record, record, "must be equal");

    assert_eq!(cache.delete(&key).unwrap(), version);

    cache.get(&key).expect_err("Must not find a record");
}

#[test]
fn insert_and_update_record() {
    let (cache, schema, _) = _setup();
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
    cache.insert(&mut foo).unwrap();
    let old_version = foo.version.unwrap();
    cache.insert(&mut bar).unwrap();

    let key = index::get_primary_key(&schema.primary_index, &foo.values);

    assert_eq!(cache.update(&key, &mut foo).unwrap(), old_version);
    assert_eq!(foo.version.unwrap(), old_version + 1);
}

fn insert_and_query_record_impl(cache: LmdbRwCache, schema: Schema, schema_name: &str) {
    let val = "bar".to_string();
    let mut record = Record::new(schema.identifier, vec![Field::String(val)], None);

    cache.insert(&mut record).unwrap();

    // Query with an expression
    let exp = query_from_filter(FilterExpression::Simple(
        "foo".to_string(),
        expression::Operator::EQ,
        Value::from("bar".to_string()),
    ));

    query_and_test(&cache, &record, schema_name, &exp);

    // Query without an expression
    query_and_test(
        &cache,
        &record,
        schema_name,
        &QueryExpression::new(None, vec![], Some(10), Skip::Skip(0)),
    );
}

#[test]
fn insert_and_query_record() {
    let (cache, schema, schema_name) = _setup();
    insert_and_query_record_impl(cache, schema, schema_name);
    let (cache, schema, schema_name) = _setup_empty_primary_index();
    insert_and_query_record_impl(cache, schema, schema_name);
}
