use criterion::Criterion;
use dozer_ingestion::connectors::snowflake::test_utils::remove_streams;
use dozer_ingestion::connectors::{get_connector, TableInfo};
use dozer_ingestion::ingestion::IngestionIterator;
use dozer_ingestion::ingestion::{IngestionConfig, Ingestor};
use dozer_types::parking_lot::RwLock;
use load_file::load_str;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

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

    group.finish();
}

pub fn main() {
    use dozer_types::models::source::Source;

    let source =
        serde_yaml::from_str::<Source>(&load_str!("../../config/tests/local/test.snowflake.yaml"))
            .unwrap();

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
}
