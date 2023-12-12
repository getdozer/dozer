use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use dozer_cache::cache::expression::{self, FilterExpression, QueryExpression, Skip};
use dozer_cache::cache::{
    test_utils, CacheManagerOptions, CacheRecord, LmdbRwCacheManager, RwCache, RwCacheManager,
    SqliteRwCache,
};
use dozer_types::serde_json::Value;
use dozer_types::types::{Field, Record};
use rusqlite::Connection;

fn insert(cache: &mut Box<dyn RwCache>, n: usize, commit_size: usize) {
    let val = format!("bar_{n}");
    let record = Record::new(vec![Field::String(val)]);

    cache.insert(&record).unwrap();

    if n % commit_size == 0 {
        cache.commit(&Default::default()).unwrap();
    }
}

fn query(cache: &mut Box<dyn RwCache>, _n: usize) -> Vec<CacheRecord> {
    let exp = QueryExpression::new(
        Some(FilterExpression::Simple(
            "foo".to_string(),
            expression::Operator::EQ,
            Value::from("bar".to_string()),
        )),
        vec![],
        Some(10),
        Skip::Skip(0),
    );

    cache.query(&exp).unwrap()
}

fn lmdb_cache(c: &mut Criterion) {
    let mut group = c.benchmark_group("lmdb");
    let (schema, secondary_indexes) = test_utils::schema_0();

    let path = std::env::var("CACHE_BENCH_PATH").unwrap_or(".dozer".to_string());
    std::fs::create_dir_all(&path).unwrap();
    let tmpdir = tempdir::TempDir::new_in(path, "bench").unwrap();
    let commit_size = std::env::var("CACHE_BENCH_COMMIT_SIZE").unwrap_or("".to_string());
    let commit_size: usize = commit_size.parse().unwrap_or(1000);

    let max_size = std::env::var("CACHE_BENCH_MAP_SIZE").unwrap_or("".to_string());
    let max_size: usize = max_size.parse().unwrap_or(49999872000);

    let cache_manager = LmdbRwCacheManager::new(CacheManagerOptions {
        max_db_size: 1000,
        max_size,
        path: Some(tmpdir.path().to_path_buf()),
        ..Default::default()
    })
    .unwrap();
    let mut cache = cache_manager
        .create_cache(
            "temp".to_string(),
            Default::default(),
            (schema, secondary_indexes),
            &Default::default(),
            Default::default(),
        )
        .unwrap();

    let iterations = std::env::var("CACHE_BENCH_ITERATIONS").unwrap_or("".to_string());
    let iterations: usize = iterations.parse().unwrap_or(1000000);

    let mut idx = 0;
    group.bench_with_input(
        BenchmarkId::new("cache_insert", iterations),
        &iterations,
        |b, &_s| {
            b.iter(|| {
                insert(&mut cache, idx, commit_size);
                idx += 1;
            })
        },
    );

    cache_manager.wait_until_indexing_catchup();

    group.bench_with_input(
        BenchmarkId::new("cache_query", iterations),
        &iterations,
        |b, &s| b.iter(|| query(&mut cache, s)),
    );
    group.finish()
}

fn sqlite_cache(c: &mut Criterion) {
    let mut group = c.benchmark_group("sqlite");
    let (schema, secondary_indexes) = test_utils::schema_0();

    let path = std::env::var("CACHE_BENCH_PATH").unwrap_or(".dozer".to_string());
    std::fs::create_dir_all(&path).unwrap();
    let tmpdir = tempdir::TempDir::new_in(path, "bench").unwrap();
    let commit_size = std::env::var("CACHE_BENCH_COMMIT_SIZE").unwrap_or("".to_string());
    let commit_size: usize = commit_size.parse().unwrap_or(1000);

    let conn = Connection::open(tmpdir.path().join("bench.db")).unwrap();
    let journal_mode: String = conn
        .pragma_update_and_check(None, "journal_mode", "WAL", |new| new.get(0))
        .unwrap();
    assert_eq!(journal_mode.to_lowercase(), "wal");
    let cache =
        SqliteRwCache::open_or_create("bench".to_owned(), Some((schema, secondary_indexes)), conn)
            .unwrap();
    let mut cache = Box::new(cache) as Box<dyn RwCache>;

    let iterations = std::env::var("CACHE_BENCH_ITERATIONS").unwrap_or("".to_string());
    let iterations: usize = iterations.parse().unwrap_or(1000000);

    let mut idx = 0;
    group.bench_with_input(
        BenchmarkId::new("cache_insert", iterations),
        &iterations,
        |b, &_s| {
            b.iter(|| {
                insert(&mut cache, idx, commit_size);
                idx += 1;
            })
        },
    );

    group.bench_with_input(
        BenchmarkId::new("cache_query", iterations),
        &iterations,
        |b, &s| b.iter(|| query(&mut cache, s)),
    );
    group.finish()
}

criterion_group!(benches, lmdb_cache, sqlite_cache);
criterion_main!(benches);
