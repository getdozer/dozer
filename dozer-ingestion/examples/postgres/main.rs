use dozer_ingestion::connectors::postgres::connector::{PostgresConfig, PostgresConnector};
use dozer_ingestion::connectors::{Connector, TableInfo};
use dozer_ingestion::errors::ConnectorError;
use dozer_ingestion::ingestion::{IngestionConfig, Ingestor};

use dozer_types::log::debug;
use std::thread;
use std::time::Instant;

fn main() {
    dozer_tracing::init_telemetry(false).unwrap();

    let (ingestor, mut iterator) = Ingestor::initialize_channel(IngestionConfig::default());
    let tables = vec![TableInfo {
        name: "users".to_string(),
        table_name: "users".to_string(),
        id: 0,
        columns: None,
    }];
    let postgres_config = PostgresConfig {
        name: "test_c".to_string(),
        tables: Some(tables.clone()),
        config: tokio_postgres::Config::default()
            .host("127.0.0.1")
            .port(5432)
            .user("postgres")
            .dbname("pagila")
            .to_owned(),
    };

    thread::spawn(move || -> Result<(), ConnectorError> {
        let connector = PostgresConnector::new(1, postgres_config);
        connector.start(None, &ingestor, tables)
    });

    let before = Instant::now();
    const BACKSPACE: char = 8u8 as char;
    let mut i = 0;
    loop {
        let _msg = iterator.next().unwrap();
        if i % 100 == 0 {
            debug!(
                "{}\rCount: {}, Elapsed time: {:.2?}",
                BACKSPACE,
                i,
                before.elapsed(),
            );
        }
        i += 1;
    }
}
