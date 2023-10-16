use std::sync::Arc;

use crate::cache::{
    expression::{self, FilterExpression, QueryExpression, Skip},
    index,
    lmdb::{cache::LmdbRwCache, indexing::IndexingThreadPool},
    test_utils::{self, query_from_filter},
    RoCache, RwCache, UpsertResult,
};
use dozer_types::{
    parking_lot::Mutex,
    serde_json::Value,
    types::{Field, Record, Schema},
};

use super::utils::create_cache;

fn _setup() -> (LmdbRwCache, Arc<Mutex<IndexingThreadPool>>, Schema) {
    let (cache, indexing_thread_pool, schema, _) = create_cache(test_utils::schema_0);
    (cache, indexing_thread_pool, schema)
}

fn _setup_empty_primary_index() -> (LmdbRwCache, Arc<Mutex<IndexingThreadPool>>, Schema) {
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
    let (mut cache, indexing_thread_pool, _) = _setup();

    assert_eq!(cache.count(&QueryExpression::with_no_limit()).unwrap(), 0);

    let record = Record::new(vec![Field::String(val.clone())]);
    let UpsertResult::Inserted { meta } = cache.insert(&record).unwrap() else {
        panic!("Must be inserted")
    };
    cache.commit().unwrap();
    indexing_thread_pool.lock().wait_until_catchup();

    assert_eq!(cache.count(&QueryExpression::with_no_limit()).unwrap(), 1);

    let key = index::get_primary_key(&[0], &[Field::String(val.clone())]);

    let get_record = cache.get(&key).unwrap().record;
    assert_eq!(get_record, record, "must be equal");

    assert_eq!(
        cache
            .delete(&Record {
                values: vec![Field::String(val)],
                lifetime: None,
            })
            .unwrap()
            .unwrap(),
        meta
    );
    cache.commit().unwrap();
    indexing_thread_pool.lock().wait_until_catchup();

    assert_eq!(cache.count(&QueryExpression::with_no_limit()).unwrap(), 0);

    cache.get(&key).expect_err("Must not find a record");

    assert_eq!(cache.query(&QueryExpression::default()).unwrap(), vec![]);
}

#[test]
fn insert_and_update_record() {
    let (mut cache, _, _) = _setup();
    let foo = Record::new(vec![Field::String("foo".to_string())]);
    let bar = Record::new(vec![Field::String("bar".to_string())]);
    let UpsertResult::Inserted { meta } = cache.insert(&foo).unwrap() else {
        panic!("Must be inserted")
    };
    cache.insert(&bar).unwrap();

    let UpsertResult::Updated { old_meta, new_meta } = cache.update(&foo, &foo).unwrap() else {
        panic!("Must be updated")
    };
    assert_eq!(old_meta, meta);
    assert_eq!(old_meta.id, new_meta.id);
    assert_eq!(old_meta.version + 1, new_meta.version);
}

fn insert_and_query_record_impl(
    mut cache: LmdbRwCache,
    indexing_thread_pool: Arc<Mutex<IndexingThreadPool>>,
) {
    let val = "bar".to_string();
    let record = Record::new(vec![Field::String(val)]);

    cache.insert(&record).unwrap();
    cache.commit().unwrap();
    indexing_thread_pool.lock().wait_until_catchup();

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
    let (cache, indexing_thread_pool, _) = _setup();
    insert_and_query_record_impl(cache, indexing_thread_pool);
    let (cache, indexing_thread_pool, _) = _setup_empty_primary_index();
    insert_and_query_record_impl(cache, indexing_thread_pool);
}

#[test]
// This test cases covers update of records when primary key changes because of value change in primary_key
fn update_record_when_primary_changes() {
    let (mut cache, _, schema) = _setup();

    let initial_values = vec![Field::String("1".into())];
    let initial_record = Record {
        values: initial_values.clone(),
        lifetime: None,
    };

    let updated_values = vec![Field::String("2".into())];
    let updated_record = Record {
        values: updated_values.clone(),
        lifetime: None,
    };

    cache.insert(&initial_record).unwrap();
    cache.commit().unwrap();

    let key = index::get_primary_key(&schema.primary_index, &initial_values);
    let record = cache.get(&key).unwrap().record;

    assert_eq!(initial_values, record.values);

    cache.update(&initial_record, &updated_record).unwrap();
    cache.commit().unwrap();

    // Primary key with old values
    let key = index::get_primary_key(&schema.primary_index, &initial_values);

    let record = cache.get(&key);

    assert!(record.is_err());

    // Primary key with updated values
    let key = index::get_primary_key(&schema.primary_index, &updated_values);
    let record = cache.get(&key).unwrap().record;

    assert_eq!(updated_values, record.values);
}

#[test]
fn test_cache_metadata() {
    let (mut cache, _, _) = _setup();
    assert!(cache.get_metadata().unwrap().is_none());
    cache.set_metadata(32).unwrap();
    cache.commit().unwrap();
    assert_eq!(cache.get_metadata().unwrap().unwrap(), 32);
}
