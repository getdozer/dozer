use criterion::Criterion;
use dozer_ingestion::connectors::postgres::test_utils::get_iterator;
use dozer_ingestion::connectors::postgres::tests::client::TestPostgresClient;
use dozer_ingestion::ingestion::IngestionIterator;
use dozer_ingestion::test_util::load_config;
use dozer_types::ingestion_types::IngestionOperation;
use dozer_types::models::app_config::Config;
use dozer_types::parking_lot::RwLock;
use dozer_types::serde_yaml;
use rand::Rng;
use std::sync::Arc;
use std::time::{Duration, Instant};

struct PostgresBench<'a> {
    client: TestPostgresClient,
    table_name: &'a str,
}

impl<'a> PostgresBench<'a> {
    pub fn new(client: TestPostgresClient, table_name: &'a str) -> PostgresBench {
        Self { client, table_name }
    }

    pub fn run_bench(&mut self, c: &mut Criterion, iterator: Arc<RwLock<IngestionIterator>>) {
        let mut group = c.benchmark_group("Ingestion postgres");

        group.warm_up_time(Duration::from_secs(1));
        group.sample_size(100);
        group.measurement_time(Duration::from_secs(100));

        group.bench_function("postgres (chunks of 10)", |b| {
            b.iter_custom(|iters| {
                self.client.insert_rows(self.table_name, iters * 10);

                let start = Instant::now();
                let mut i = 0;
                while i < iters * 10 {
                    let m = iterator.write().next();
                    if let Some((_, IngestionOperation::OperationEvent(_))) = m {
                        i += 1;
                    }
                }

                start.elapsed()
            });
        });

        group.finish();
    }
}

pub fn main() {
    let config = serde_yaml::from_str::<Config>(load_config("test.postgres.yaml")).unwrap();
    let connection = config.connections.get(0).unwrap().clone();

    let mut rng = rand::thread_rng();
    let table_name = format!("products_test_{}", rng.gen::<u32>());

    let mut client =
        TestPostgresClient::new(&connection.authentication.to_owned().unwrap_or_default());

    client.create_simple_table("public", &table_name);

    let iterator = get_iterator(connection, table_name.clone());
    let mut criterion = Criterion::default().configure_from_args();
    let mut bench = PostgresBench::new(client, &table_name);
    bench.run_bench(&mut criterion, iterator);
}
