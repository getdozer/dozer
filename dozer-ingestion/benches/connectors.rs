use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use dozer_types::serde_yaml;
use helper::TestConfig;
use tokio::runtime::Runtime;
mod helper;
fn connectors(criter: &mut Criterion) {
    let configs = load_test_config();

    Runtime::new().unwrap().block_on(async {
        for config in configs {
            let mut iterator = helper::get_connection_iterator(config.clone()).await;
            let pb = helper::get_progress();
            let mut count = 0;
            criter.bench_with_input(
                BenchmarkId::new(config.connection.name, config.size),
                &config.size,
                |b, _| {
                    b.iter(|| {
                        iterator.next();
                        count += 1;
                        if count % 100 == 0 {
                            pb.set_position(count as u64);
                        }
                    })
                },
            );
        }
    });
}

pub fn load_test_config() -> Vec<TestConfig> {
    let test_config = include_str!("./connectors.yaml");
    serde_yaml::from_str::<Vec<TestConfig>>(test_config).unwrap()
}

criterion_group!(benches, connectors);
criterion_main!(benches);
