use crate::cache::{
    expression::{self, FilterExpression, QueryExpression, Skip},
    index,
    lmdb::{cache::LmdbRwCache, indexing::IndexingThreadPool},
    test_utils::{self, query_from_filter},
    RoCache, RwCache,
};
use dozer_types::{
    serde_json::Value,
    types::{Field, Record, Schema},
};

use super::utils::create_cache;

fn _setup() -> (LmdbRwCache, IndexingThreadPool, Schema) {
    let (cache, indexing_thread_pool, schema, _) = create_cache(test_utils::schema_0);
    (cache, indexing_thread_pool, schema)
}

fn _setup_empty_primary_index() -> (LmdbRwCache, IndexingThreadPool, Schema) {
    let (cache, indexing_thread_pool, schema, _) =
        create_cache(test_utils::schema_empty_primary_index);
    (cache, indexing_thread_pool, schema)
}

fn query_and_test(cache: &dyn RwCache, inserted_record: &Record, exp: &QueryExpression) {
    let records = cache.query(exp).unwrap();
    assert_eq!(records[0].record, inserted_record.clone(), "must be equal");
}

#[test]
fn get_schema() {
    let (cache, _, schema) = _setup();

    let get_schema = &cache.get_schema().0;
    assert_eq!(get_schema, &schema, "must be equal");
}

#[test]
fn insert_get_and_delete_record() {
    let val = "bar".to_string();
    let (cache, mut indexing_thread_pool, schema) = _setup();

    assert_eq!(cache.count(&QueryExpression::with_no_limit()).unwrap(), 0);

    let mut record = Record::new(schema.identifier, vec![Field::String(val.clone())], None);
    cache.insert(&mut record).unwrap();
    cache.commit().unwrap();
    indexing_thread_pool.wait_until_catchup();

    assert_eq!(cache.count(&QueryExpression::with_no_limit()).unwrap(), 1);

    let version = record.version.unwrap();

    let key = index::get_primary_key(&[0], &[Field::String(val)]);

    let get_record = cache.get(&key).unwrap().record;
    assert_eq!(get_record, record, "must be equal");

    assert_eq!(cache.delete(&key).unwrap(), version);
    cache.commit().unwrap();
    indexing_thread_pool.wait_until_catchup();

    assert_eq!(cache.count(&QueryExpression::with_no_limit()).unwrap(), 0);

    cache.get(&key).expect_err("Must not find a record");

    assert_eq!(cache.query(&QueryExpression::default()).unwrap(), vec![]);
}

#[test]
fn insert_and_update_record() {
    let (cache, _, schema) = _setup();
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

    assert_eq!(cache.update(&key, &mut foo).unwrap().0, Some(old_version));
    assert_eq!(foo.version.unwrap(), old_version + 1);
}

fn insert_and_query_record_impl(
    cache: LmdbRwCache,
    mut indexing_thread_pool: IndexingThreadPool,
    schema: Schema,
) {
    let val = "bar".to_string();
    let mut record = Record::new(schema.identifier, vec![Field::String(val)], None);

    cache.insert(&mut record).unwrap();
    cache.commit().unwrap();
    indexing_thread_pool.wait_until_catchup();

    // Query with an expression
    let exp = query_from_filter(FilterExpression::Simple(
        "foo".to_string(),
        expression::Operator::EQ,
        Value::from("bar".to_string()),
    ));

    query_and_test(&cache, &record, &exp);

    // Query without an expression
    query_and_test(
        &cache,
        &record,
        &QueryExpression::new(None, vec![], Some(10), Skip::Skip(0)),
    );
}

#[test]
fn insert_and_query_record() {
    let (cache, indexing_thread_pool, schema) = _setup();
    insert_and_query_record_impl(cache, indexing_thread_pool, schema);
    let (cache, indexing_thread_pool, schema) = _setup_empty_primary_index();
    insert_and_query_record_impl(cache, indexing_thread_pool, schema);
}
