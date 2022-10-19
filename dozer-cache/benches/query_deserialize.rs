use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use dozer_cache::cache::expression::QueryExpression;
use dozer_types::serde_json::{self, json, Value};
use rand::{
    distributions::{Alphanumeric, DistString},
    Rng,
};

fn deserialize(n: usize) -> anyhow::Result<()> {
    let comparision_key = vec![
        "$eq",
        "$lt",
        "$lte",
        "$gt",
        "$gte",
        "$contains",
        "$matchesany",
        "$matchesall",
    ];
    let mut rng = rand::thread_rng();
    let no_simple_exp = 3;
    let simple_ex: Vec<Value> = (0..no_simple_exp)
        .collect::<Vec<_>>()
        .iter()
        .map(|_| {
            let dice = rng.gen_range(0..comparision_key.len() - 1);
            let sample_string = Alphanumeric.sample_string(&mut rand::thread_rng(), 8);
            json!({ sample_string: {comparision_key[dice]: n}})
        })
        .collect();
    let complex_ex = json!({"$filter":  { "$and": simple_ex }, "$order_by": {"field_name": "a_b", "direction": "asc"} });
    serde_json::from_value::<QueryExpression>(complex_ex)?;
    Ok(())
}

fn query_deserialize(c: &mut Criterion) {
    let size: usize = 1000000;
    c.bench_with_input(
        BenchmarkId::new("query_deserialize", size),
        &size,
        |b, &s| {
            b.iter(|| {
                deserialize(s).unwrap();
            })
        },
    );
}
criterion_group!(benches, query_deserialize);
criterion_main!(benches);
