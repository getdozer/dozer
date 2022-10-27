use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use dozer_cache::cache::expression::{self, FilterExpression, QueryExpression};
use dozer_cache::cache::LmdbCache;
use dozer_cache::cache::{index, test_utils, Cache};
use dozer_types::serde_json::Value;
use dozer_types::types::{Field, Schema};
use std::sync::Arc;

fn insert(cache: Arc<LmdbCache>, schema: Schema, n: usize) {
    let val = format!("bar_{}", n);
    test_utils::insert_rec_1(&cache, &schema, (n as i64, val, (n + 1000) as i64));
}

fn get(cache: Arc<LmdbCache>, n: usize) {
    let key = index::get_primary_key(&[0], &[Field::Int(n as i64)]);
    let _get_record = cache.get(&key).unwrap();
}

fn query(cache: Arc<LmdbCache>, n: usize) {
    let exp = QueryExpression::new(
        Some(FilterExpression::Simple(
            "a".to_string(),
            expression::Operator::EQ,
            Value::from(n),
        )),
        vec![],
        10,
        0,
    );

    let _get_record = cache.query("benches", &exp).unwrap();
}

fn cache(c: &mut Criterion) {
    let schema = test_utils::schema_1();
    let cache = Arc::new(LmdbCache::new(true));

    cache.insert_schema("benches", &schema).unwrap();

    let size: usize = 1000000;
    c.bench_with_input(BenchmarkId::new("cache_insert", size), &size, |b, &s| {
        b.iter(|| {
            insert(Arc::clone(&cache), schema.clone(), s);
        })
    });

    c.bench_with_input(BenchmarkId::new("cache_get", size), &size, |b, &s| {
        b.iter(|| {
            get(Arc::clone(&cache), s);
        })
    });

    c.bench_with_input(BenchmarkId::new("cache_query", size), &size, |b, &s| {
        b.iter(|| query(Arc::clone(&cache), s))
    });
}

criterion_group!(benches, cache);
criterion_main!(benches);
