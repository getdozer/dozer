#[cfg(feature = "kafka_test")]
use criterion::Criterion;
#[cfg(feature = "kafka_test")]
use dozer_ingestion::connectors::kafka::test_utils::load_config;
#[cfg(feature = "kafka_test")]
use dozer_ingestion::connectors::postgres::connection::helper::{connect, map_connection_config};
#[cfg(feature = "kafka_test")]
use dozer_ingestion::connectors::{get_connector, TableInfo};
#[cfg(feature = "kafka_test")]
use dozer_ingestion::ingestion::{IngestionConfig, IngestionIterator, Ingestor};
#[cfg(feature = "kafka_test")]
use dozer_types::ingestion_types::IngestionOperation;

#[cfg(feature = "kafka_test")]
use dozer_types::parking_lot::RwLock;
#[cfg(feature = "kafka_test")]
use dozer_types::rust_decimal::Decimal;

#[cfg(feature = "kafka_test")]
use postgres::Client;

#[cfg(feature = "kafka_test")]
use reqwest::header::{ACCEPT, CONTENT_TYPE};
#[cfg(feature = "kafka_test")]
use std::fmt::Write;
#[cfg(feature = "kafka_test")]
use std::sync::Arc;
#[cfg(feature = "kafka_test")]
use std::thread;
#[cfg(feature = "kafka_test")]
use std::time::{Duration, Instant};

#[cfg(feature = "kafka_test")]
struct DebeziumBench {
    client: Client,
}

#[cfg(feature = "kafka_test")]
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

        self.client
            .query(&query, &[])
            .unwrap();
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

#[cfg(feature = "kafka_test")]
pub fn main() {
    let config = load_config("./config/test.debezium.yaml".to_string()).unwrap();

    let source = config.source;
    let postgres_config = map_connection_config(&config.postgres_source_authentication).unwrap();

    let mut client = connect(postgres_config).unwrap();

    let connector_client = reqwest::blocking::Client::new();

    connector_client
        .delete(format!(
            "{}{}",
            config.debezium_connector_url,
            "dozer-postgres-connector".to_string()
        ))
        .send()
        .unwrap()
        .text()
        .unwrap();

    client
        .query("DROP TABLE IF EXISTS products_test", &[])
        .unwrap();

    client
        .query(
            "CREATE TABLE products_test
(
    id          SERIAL
        PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    description VARCHAR(512),
    weight      DOUBLE PRECISION
);",
            &[],
        )
        .unwrap();

    let content =
        std::fs::read_to_string("./tests/connectors/debezium/register-postgres.test.json").unwrap();

    connector_client
        .post(&config.debezium_connector_url)
        .body(content.clone())
        .header(CONTENT_TYPE, "application/json")
        .header(ACCEPT, "application/json")
        .send()
        .unwrap()
        .text()
        .unwrap();

    let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());

    thread::spawn(move || {
        let tables: Vec<TableInfo> = vec![TableInfo {
            name: source.table_name.clone(),
            id: 0,
            columns: None,
        }];

        let mut connector = get_connector(source.connection).unwrap();
        connector.initialize(ingestor, Some(tables)).unwrap();
        let _ = connector.start();
    });

    let mut criterion = Criterion::default().configure_from_args();
    let mut bench = DebeziumBench::new(client);
    bench.run_bench(&mut criterion, iterator);
}

#[cfg(not(feature = "kafka_test"))]
fn main() {}
