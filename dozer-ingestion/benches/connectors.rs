use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use dozer_types::log::error;
use dozer_types::serde_yaml;

use dozer_ingestion::ingestion::{IngestionConfig, IngestionIterator, Ingestor};
use dozer_orchestrator::Connection;
use dozer_types::indicatif::{ProgressBar, ProgressStyle};
use dozer_types::serde::{self, Deserialize, Serialize};
use tokio::runtime::Runtime;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(crate = "self::serde")]
pub struct TestConfig {
    pub connection: Connection,
    pub tables_filter: Option<Vec<String>>,
    #[serde(default = "default_size")]
    pub size: usize,
}
fn default_size() -> usize {
    1000
}

fn get_progress() -> ProgressBar {
    let pb = ProgressBar::new_spinner();

    pb.set_style(
        ProgressStyle::with_template("{spinner:.blue} {msg}: {pos}: {per_sec}")
            .unwrap()
            // For more spinners check out the cli-spinners project:
            // https://github.com/sindresorhus/cli-spinners/blob/master/spinners.json
            .tick_strings(&[
                "▹▹▹▹▹",
                "▸▹▹▹▹",
                "▹▸▹▹▹",
                "▹▹▸▹▹",
                "▹▹▹▸▹",
                "▹▹▹▹▸",
                "▪▪▪▪▪",
            ]),
    );
    pb
}

fn load_test_config() -> Vec<TestConfig> {
    let test_config = include_str!("./connectors.yaml");
    serde_yaml::from_str::<Vec<TestConfig>>(&test_config).unwrap()
}
fn connectors(criter: &mut Criterion) {
    let configs = load_test_config();

    Runtime::new().unwrap().block_on(async {
        for config in configs {
            let mut iterator = get_connection_iterator(config.clone()).await;
            let pb = get_progress();
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

pub async fn get_connection_iterator(config: TestConfig) -> IngestionIterator {
    let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());
    std::thread::spawn(move || {
        let grpc_connector = dozer_ingestion::connectors::get_connector(config.connection).unwrap();

        let mut tables = grpc_connector.get_tables(None).unwrap();
        if let Some(tables_filter) = config.tables_filter {
            tables = tables
                .iter()
                .filter(|t| tables_filter.contains(&t.name))
                .cloned()
                .collect();
        }

        let res = grpc_connector.start(None, &ingestor, tables);
        if let Err(e) = res {
            error!("Error: {:?}", e);
        }
    });
    iterator
}

criterion_group!(benches, connectors);
criterion_main!(benches);
