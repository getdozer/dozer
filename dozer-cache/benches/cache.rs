use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use dozer_cache::cache::expression::{self, FilterExpression, QueryExpression, SortOptions};
use dozer_cache::cache::{index, test_utils, Cache};
use dozer_cache::cache::{CacheOptions, LmdbCache};
use dozer_types::serde_json::Value;
use dozer_types::types::{Field, Record, Schema};
use std::sync::Arc;

fn insert(cache: Arc<LmdbCache>, schema: Schema, n: usize) {
    let val = format!("bar_{}", n);

    let record = Record::new(schema.identifier, vec![Field::String(val.clone())]);

    cache.insert(&record).unwrap();
    let key = index::get_primary_key(&[0], &[Field::String(val)]);

    let _get_record = cache.get(&key).unwrap();
}

fn get(cache: Arc<LmdbCache>, n: usize) {
    let val = format!("bar_{}", n);
    let key = index::get_primary_key(&[0], &[Field::String(val)]);
    let _get_record = cache.get(&key).unwrap();
}

fn query(cache: Arc<LmdbCache>, _n: usize) {
    let exp = QueryExpression::new(
        Some(FilterExpression::Simple(
            "foo".to_string(),
            expression::Operator::EQ,
            Value::from("bar".to_string()),
        )),
        vec![],
        10,
        0,
    );

    let _get_record = cache.query("benches", &exp).unwrap();
}

fn cache(c: &mut Criterion) {
    let (schema, secondary_indexes) = test_utils::schema_0();
    let cache = Arc::new(LmdbCache::new(CacheOptions::default()).unwrap());

    cache
        .insert_schema("benches", &schema, &secondary_indexes)
        .unwrap();

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
