use anyhow::Ok;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use dozer_cache::cache::expression::{self, Expression};
use std::sync::Arc;
use tokio::runtime::Runtime;

use dozer_cache::cache::lmdb::cache::LmdbCache;
use dozer_cache::cache::{get_primary_key, Cache};
use dozer_schema::{
    registry::{SchemaRegistryClient, _serve_channel, client},
    test_helper::init_schema,
};
use dozer_types::types::{Field, Record, Schema};

async fn insert(cache: Arc<LmdbCache>, schema: Schema, n: usize) -> anyhow::Result<()> {
    let val = format!("bar_{}", n).to_string();

    let record = Record::new(schema.identifier.clone(), vec![Field::String(val.clone())]);

    cache.insert_with_schema(&record, &schema, "benches")?;
    let key = get_primary_key(&vec![0], &vec![Field::String(val)]);

    let _get_record = cache.get(&key)?;
    Ok(())
}

async fn get(cache: Arc<LmdbCache>, n: usize) -> anyhow::Result<()> {
    let val = format!("bar_{}", n).to_string();
    let key = get_primary_key(&vec![0], &vec![Field::String(val)]);
    let _get_record = cache.get(&key)?;
    Ok(())
}

async fn query(cache: Arc<LmdbCache>, n: usize) -> anyhow::Result<()> {
    let exp = Expression::Simple(
        "foo".to_string(),
        expression::Comparator::EQ,
        Field::String(format!("bar_{}", n).to_string()),
    );

    let _get_record = cache.query(&"benches", &exp, 50)?;
    Ok(())
}

fn cache(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let (cache, schema) = rt.block_on(async {
        let client_transport = _serve_channel().unwrap();
        let client = Arc::new(
            SchemaRegistryClient::new(client::Config::default(), client_transport).spawn(),
        );
        let schema = init_schema(client.clone()).await;
        let cache = Arc::new(LmdbCache::new(true));
        (cache, schema)
    });

    let size: usize = 1000000;

    c.bench_with_input(BenchmarkId::new("cache_insert", size), &size, |b, &s| {
        b.iter(|| {
            rt.block_on(async { insert(Arc::clone(&cache), schema.clone(), s).await })
                .unwrap();
        })
    });

    c.bench_with_input(BenchmarkId::new("cache_get", size), &size, |b, &s| {
        b.iter(|| {
            rt.block_on(async { get(Arc::clone(&cache), s).await })
                .unwrap();
        })
    });

    c.bench_with_input(BenchmarkId::new("cache_query", size), &size, |b, &s| {
        b.iter(|| {
            rt.block_on(async { query(Arc::clone(&cache), s).await })
                .unwrap();
        })
    });
}

criterion_group!(benches, cache);
criterion_main!(benches);
