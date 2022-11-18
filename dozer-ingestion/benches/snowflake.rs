#[cfg(feature = "snowflake")]
use criterion::Criterion;
#[cfg(feature = "snowflake")]
use std::sync::Arc;
#[cfg(feature = "snowflake")]
use std::thread;

#[cfg(feature = "snowflake")]
use dozer_ingestion::ingestion::{IngestionIterator};

#[cfg(feature = "snowflake")]
use dozer_ingestion::connectors::snowflake::test_utils::remove_streams;
#[cfg(feature = "snowflake")]
use dozer_types::parking_lot::RwLock;

#[cfg(feature = "snowflake")]
use std::time::Duration;
#[cfg(feature = "snowflake")]
use dozer_ingestion::connectors::{get_connector, TableInfo};
#[cfg(feature = "snowflake")]
use dozer_ingestion::ingestion::{IngestionConfig, Ingestor};
#[cfg(feature = "snowflake")]
use dozer_ingestion::ingestion::test_utils::load_config;

#[cfg(feature = "snowflake")]
fn snowflake(c: &mut Criterion, iterator: Arc<RwLock<IngestionIterator>>) {
    let mut group = c.benchmark_group("Ingestion");

    group.warm_up_time(Duration::from_secs(1));
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("snowflake (chunks of 10000)", |b| {
        b.iter(|| {
            let mut i = 0;
            while i < 10000 {
                iterator.write().next();
                i += 1;
            }
        });
    });
}

#[cfg(feature = "snowflake")]
pub fn main() {
    let source = load_config("../dozer-config.test.snowflake.yaml".to_string()).unwrap();

    remove_streams(source.connection.clone(), &source.table_name).unwrap();

    let config = IngestionConfig::default();

    let (ingestor, iterator) = Ingestor::initialize_channel(config);

    thread::spawn(|| {
        let tables: Vec<TableInfo> = vec![TableInfo {
            name: source.table_name,
            id: 0,
            columns: None,
        }];

        let mut connector = get_connector(source.connection).unwrap();
        connector.initialize(ingestor, Some(tables)).unwrap();
        connector.start().unwrap();
    });

    let mut criterion = Criterion::default().configure_from_args();
    snowflake(&mut criterion, iterator);

    Criterion::default().configure_from_args().final_summary();
}

#[cfg(not(feature = "snowflake"))]
pub fn main() {}
