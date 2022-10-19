use anyhow::Ok;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use dozer_cache::cache::expression::{self, FilterExpression, QueryExpression};
use dozer_cache::cache::LmdbCache;
use dozer_cache::cache::{index, test_utils, Cache};
use dozer_types::serde_json::Value;
use dozer_types::types::{Field, Record, Schema};
use std::sync::Arc;

fn insert(cache: Arc<LmdbCache>, schema: Schema, n: usize) -> anyhow::Result<()> {
    let val = format!("bar_{}", n);

    let record = Record::new(schema.identifier, vec![Field::String(val.clone())]);

    cache.insert(&record)?;
    let key = index::get_primary_key(&[0], &[Field::String(val)]);

    let _get_record = cache.get(&key)?;
    Ok(())
}

fn get(cache: Arc<LmdbCache>, n: usize) -> anyhow::Result<()> {
    let val = format!("bar_{}", n);
    let key = index::get_primary_key(&[0], &[Field::String(val)]);
    let _get_record = cache.get(&key)?;
    Ok(())
}

fn query(cache: Arc<LmdbCache>, _n: usize) -> anyhow::Result<()> {
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

    let _get_record = cache.query("benches", &exp)?;
    Ok(())
}

fn cache(c: &mut Criterion) {
    let schema = test_utils::schema_0();
    let cache = Arc::new(LmdbCache::new(true));

    cache.insert_schema("benches", &schema).unwrap();

    let size: usize = 1000000;
    c.bench_with_input(BenchmarkId::new("cache_insert", size), &size, |b, &s| {
        b.iter(|| {
            insert(Arc::clone(&cache), schema.clone(), s).unwrap();
        })
    });

    c.bench_with_input(BenchmarkId::new("cache_get", size), &size, |b, &s| {
        b.iter(|| {
            get(Arc::clone(&cache), s).unwrap();
        })
    });

    c.bench_with_input(BenchmarkId::new("cache_query", size), &size, |b, &s| {
        b.iter(|| {
            query(Arc::clone(&cache), s).unwrap();
        })
    });
}

criterion_group!(benches, cache);
criterion_main!(benches);
