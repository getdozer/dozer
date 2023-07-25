use std::path::Path;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use dozer_cache::cache::expression::{self, FilterExpression, QueryExpression, Skip};
use dozer_cache::cache::{
    test_utils, CacheManagerOptions, LmdbRwCacheManager, RwCache, RwCacheManager,
};
use dozer_types::parking_lot::Mutex;
use dozer_types::serde_json::Value;
use dozer_types::types::{Field, Record};

fn insert(cache: &Mutex<Box<dyn RwCache>>, n: usize, commit_size: usize) {
    let mut cache = cache.lock();

    let val = format!("bar_{n}");
    let mut record = Record::new(vec![Field::String(val)]);

    cache.insert(&mut record).unwrap();

    if n % commit_size == 0 {
        cache.commit().unwrap();
    }
}

fn query(cache: &Mutex<Box<dyn RwCache>>, _n: usize) {
    let cache = cache.lock();
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

    let _get_record = cache.query(&exp).unwrap();
}

fn cache(c: &mut Criterion) {
    let (schema, secondary_indexes) = test_utils::schema_0();

    let path = std::env::var("CACHE_BENCH_PATH").unwrap_or(".dozer".to_string());
    let commit_size = std::env::var("CACHE_BENCH_COMMIT_SIZE").unwrap_or("".to_string());
    let commit_size: usize = commit_size.parse().unwrap_or(1000);

    let max_size = std::env::var("CACHE_BENCH_MAP_SIZE").unwrap_or("".to_string());
    let max_size: usize = max_size.parse().unwrap_or(49999872000);

    let cache_manager = LmdbRwCacheManager::new(CacheManagerOptions {
        max_db_size: 1000,
        max_size,
        path: Some(Path::new(&path).to_path_buf()),
        ..Default::default()
    })
    .unwrap();
    let cache = Mutex::new(
        cache_manager
            .create_cache(
                Default::default(),
                schema,
                secondary_indexes,
                &Default::default(),
                Default::default(),
            )
            .unwrap(),
    );

    let iterations = std::env::var("CACHE_BENCH_ITERATIONS").unwrap_or("".to_string());
    let iterations: usize = iterations.parse().unwrap_or(1000000);

    let mut idx = 0;
    c.bench_with_input(
        BenchmarkId::new("cache_insert", iterations),
        &iterations,
        |b, &_s| {
            b.iter(|| {
                insert(&cache, idx, commit_size);
                idx += 1;
            })
        },
    );

    c.bench_with_input(
        BenchmarkId::new("cache_query", iterations),
        &iterations,
        |b, &s| b.iter(|| query(&cache, s)),
    );
}

criterion_group!(benches, cache);
criterion_main!(benches);
