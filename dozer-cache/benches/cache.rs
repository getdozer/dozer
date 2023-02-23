use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use dozer_cache::cache::expression::{self, FilterExpression, QueryExpression, Skip};
use dozer_cache::cache::{index, test_utils, CacheManager, LmdbCacheManager, RwCache};
use dozer_types::serde_json::Value;
use dozer_types::types::{Field, Record, Schema};

fn insert(cache: &dyn RwCache, schema: &Schema, n: usize) {
    let val = format!("bar_{n}");

    let mut record = Record::new(schema.identifier, vec![Field::String(val.clone())], None);

    cache.insert(&mut record).unwrap();
    let key = index::get_primary_key(&[0], &[Field::String(val)]);

    let _get_record = cache.get(&key).unwrap();
}

fn delete(cache: &dyn RwCache, n: usize) {
    let val = format!("bar_{n}");
    let key = index::get_primary_key(&[0], &[Field::String(val)]);
    let _ = cache.delete(&key);
}

fn get(cache: &dyn RwCache, n: usize) {
    let val = format!("bar_{n}");
    let key = index::get_primary_key(&[0], &[Field::String(val)]);
    let _get_record = cache.get(&key).unwrap();
}

const SCHEMA_NAME: &str = "benches";

fn query(cache: &dyn RwCache, _n: usize) {
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

    let _get_record = cache.query(SCHEMA_NAME, &exp).unwrap();
}

fn cache(c: &mut Criterion) {
    let (schema, secondary_indexes) = test_utils::schema_0();
    let cache_manager = LmdbCacheManager::new(Default::default()).unwrap();
    let cache = cache_manager
        .create_cache(vec![(
            SCHEMA_NAME.to_string(),
            schema.clone(),
            secondary_indexes,
        )])
        .unwrap();

    let size: usize = 1000000;
    c.bench_with_input(BenchmarkId::new("cache_insert", size), &size, |b, &s| {
        b.iter_batched(
            || delete(&*cache, s),
            |_| insert(&*cache, &schema, s),
            criterion::BatchSize::NumIterations(1),
        )
    });

    c.bench_with_input(BenchmarkId::new("cache_get", size), &size, |b, &s| {
        b.iter(|| {
            get(&*cache, s);
        })
    });

    c.bench_with_input(BenchmarkId::new("cache_query", size), &size, |b, &s| {
        b.iter(|| query(&*cache, s))
    });
}

criterion_group!(benches, cache);
criterion_main!(benches);
