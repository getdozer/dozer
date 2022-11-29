#[cfg(feature = "kafka_debezium_e2e_tests")]
use criterion::Criterion;

#[cfg(feature = "kafka_debezium_e2e_tests")]
use dozer_ingestion::ingestion::IngestionIterator;
#[cfg(feature = "kafka_debezium_e2e_tests")]
use dozer_types::ingestion_types::IngestionOperation;

#[cfg(feature = "kafka_debezium_e2e_tests")]
use dozer_types::parking_lot::RwLock;
#[cfg(feature = "kafka_debezium_e2e_tests")]
use dozer_types::rust_decimal::Decimal;

#[cfg(feature = "kafka_debezium_e2e_tests")]
use postgres::Client;

#[cfg(feature = "kafka_debezium_e2e_tests")]
use std::fmt::Write;
#[cfg(feature = "kafka_debezium_e2e_tests")]
use std::sync::Arc;

#[cfg(feature = "kafka_debezium_e2e_tests")]
use std::time::{Duration, Instant};

#[cfg(feature = "kafka_debezium_e2e_tests")]
use dozer_ingestion::connectors::kafka::test_utils::get_iterator_and_client;

#[cfg(feature = "kafka_debezium_e2e_tests")]
struct DebeziumBench {
    client: Client,
}

#[cfg(feature = "kafka_debezium_e2e_tests")]
impl DebeziumBench {
    pub fn new(client: Client) -> DebeziumBench {
        Self { client }
    }

    pub fn insert_rows(&mut self, count: u64) {
        let mut buf = String::new();
        for i in 0..count {
            if i > 0 {
                buf.write_str(",").unwrap();
            }
            buf.write_fmt(format_args!(
                "(\'Product {}\',\'Product {} description\',{})",
                i,
                i,
                Decimal::new((i * 41) as i64, 2)
            ))
            .unwrap();
        }

        let query = format!(
            "insert into products_test(name, description, weight) values {}",
            buf,
        );

        self.client.query(&query, &[]).unwrap();
    }

    pub fn run_bench(&mut self, c: &mut Criterion, iterator: Arc<RwLock<IngestionIterator>>) {
        let mut group = c.benchmark_group("Ingestion debezium");

        group.warm_up_time(Duration::from_secs(1));
        group.sample_size(100);
        group.measurement_time(Duration::from_secs(100));

        group.bench_function("debezium (chunks of 10)", |b| {
            b.iter_custom(|iters| {
                self.insert_rows(iters * 10);

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

#[cfg(feature = "kafka_debezium_e2e_tests")]
pub fn main() {
    let (iterator, client) = get_iterator_and_client("", "products_test".to_string());

    let mut criterion = Criterion::default().configure_from_args();
    let mut bench = DebeziumBench::new(client);
    bench.run_bench(&mut criterion, iterator);
}

#[cfg(not(feature = "kafka_debezium_e2e_tests"))]
fn main() {}
