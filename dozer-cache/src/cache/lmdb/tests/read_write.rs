use crate::cache::expression::{FilterExpression, Operator, QueryExpression};
use crate::cache::lmdb::cache::{CacheCommonOptions, CacheWriteOptions, LmdbRoCache, LmdbRwCache};
use crate::cache::{lmdb::tests::utils as lmdb_utils, test_utils, RoCache, RwCache};
use dozer_types::serde_json::Value;
use dozer_types::types::Field;
use tempdir::TempDir;
#[test]
fn read_and_write() {
    let path = TempDir::new("dozer").unwrap();
    let path = (path.path().to_path_buf(), "cache".to_string());

    // write and read from cache from two different threads.

    let (schema, secondary_indexes) = test_utils::schema_1();
    let cache_writer = LmdbRwCache::create(
        schema.clone(),
        secondary_indexes,
        CacheCommonOptions {
            max_readers: 1,
            max_db_size: 100,
            path: Some(path.clone()),
            intersection_chunk_size: 1,
        },
        CacheWriteOptions {
            max_size: 1024 * 1024,
        },
    )
    .unwrap();

    let items = vec![
        (1, Some("a".to_string()), Some(521)),
        (2, Some("a".to_string()), None),
        (3, None, Some(521)),
        (4, None, None),
    ];

    for val in items.clone() {
        lmdb_utils::insert_rec_1(&cache_writer, &schema, val.clone());
    }
    cache_writer.commit(&Default::default()).unwrap();

    let read_options = CacheCommonOptions {
        path: Some(path),
        ..Default::default()
    };
    let cache_reader = LmdbRoCache::new(read_options).unwrap();
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
