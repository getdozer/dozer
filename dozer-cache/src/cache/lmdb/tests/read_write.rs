use crate::cache::expression::{FilterExpression, Operator, QueryExpression};
use crate::cache::lmdb::{CacheCommonOptions, CacheOptionsKind, CacheWriteOptions};
use crate::cache::CacheReadOptions;
use crate::cache::{lmdb::tests::utils as lmdb_utils, test_utils, Cache, CacheOptions, LmdbCache};
use dozer_types::serde_json::Value;
use dozer_types::types::Field;
use tempdir::TempDir;
#[test]
fn read_and_write() {
    let path = TempDir::new("dozer").unwrap().path().join("cache");

    // write and read from cache from two different threads.

    let cache_writer = LmdbCache::new(CacheOptions {
        common: CacheCommonOptions {
            max_readers: 1,
            max_db_size: 100,
            path: Some(path.clone()),
            intersection_chunk_size: 1,
        },
        kind: CacheOptionsKind::Write(CacheWriteOptions {
            max_size: 1024 * 1024,
        }),
    })
    .unwrap();

    let (schema, secondary_indexes) = test_utils::schema_1();

    cache_writer
        .insert_schema("sample", &schema, &secondary_indexes)
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

    let read_options = CacheOptions {
        common: CacheCommonOptions {
            path: Some(path),
            ..Default::default()
        },
        kind: CacheOptionsKind::ReadOnly(CacheReadOptions {}),
    };
    let cache_reader = LmdbCache::new(read_options).unwrap();
    for (a, b, c) in items {
        let rec = cache_reader.get(&Field::Int(a).encode()).unwrap();
        let values = vec![
            Field::Int(a),
            b.map_or(Field::Null, Field::String),
            c.map_or(Field::Null, Field::Int),
        ];
        assert_eq!(rec.values, values, "should be equal");
    }
    let records = cache_reader
        .query(
            "sample",
            &QueryExpression {
                filter: Some(FilterExpression::Simple(
                    "a".to_string(),
                    Operator::EQ,
                    Value::from(1),
                )),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(records.len(), 1);
}
