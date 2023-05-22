use std::sync::Arc;

use crate::cache::expression::{FilterExpression, Operator, QueryExpression};
use crate::cache::lmdb::cache::{CacheOptions, LmdbRoCache, LmdbRwCache};
use crate::cache::lmdb::indexing::IndexingThreadPool;
use crate::cache::{lmdb::tests::utils as lmdb_utils, test_utils, RoCache, RwCache};
use dozer_types::parking_lot::Mutex;
use dozer_types::serde_json::Value;
use dozer_types::types::Field;
use tempdir::TempDir;

#[test]
fn read_and_write() {
    let path = TempDir::new("dozer").unwrap();
    let path = (path.path().to_path_buf(), Default::default());

    // write and read from cache from two different threads.

    let schema = test_utils::schema_1();
    let indexing_thread_pool = Arc::new(Mutex::new(IndexingThreadPool::new(1)));
    let mut cache_writer = LmdbRwCache::new(
        Some(&schema),
        None,
        &CacheOptions {
            max_readers: 2,
            max_db_size: 100,
            max_size: 1024 * 1024,
            path: Some(path.clone()),
            intersection_chunk_size: 1,
        },
        Default::default(),
        indexing_thread_pool.clone(),
    )
    .unwrap();

    let items = vec![
        (1, Some("a".to_string()), Some(521)),
        (2, Some("a".to_string()), None),
        (3, None, Some(521)),
        (4, None, None),
    ];

    for val in items.clone() {
        lmdb_utils::insert_rec_1(&mut cache_writer, &schema.0, val.clone());
    }
    cache_writer.commit().unwrap();

    indexing_thread_pool.lock().wait_until_catchup();

    let read_options = CacheOptions {
        path: Some(path),
        ..Default::default()
    };
    let cache_reader = LmdbRoCache::new(&read_options).unwrap();
    for (a, b, c) in items {
        let rec = cache_reader.get(&Field::Int(a).encode()).unwrap();
        let values = vec![
            Field::Int(a),
            b.map_or(Field::Null, Field::String),
            c.map_or(Field::Null, Field::Int),
        ];
        assert_eq!(rec.record.values, values, "should be equal");
    }
    let records = cache_reader
        .query(&QueryExpression {
            filter: Some(FilterExpression::Simple(
                "a".to_string(),
                Operator::EQ,
                Value::from(1),
            )),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(records.len(), 1);
}
