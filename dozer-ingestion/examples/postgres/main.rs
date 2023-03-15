use dozer_ingestion::connectors::postgres::connector::{PostgresConfig, PostgresConnector};
use dozer_ingestion::connectors::{Connector, TableIdentifier};
use dozer_ingestion::errors::ConnectorError;
use dozer_ingestion::ingestion::{IngestionConfig, Ingestor};

use dozer_types::log::debug;
use std::thread;
use std::time::Instant;

fn main() {
    dozer_tracing::init_telemetry(None, None);

    let (ingestor, mut iterator) = Ingestor::initialize_channel(IngestionConfig::default());
    let postgres_config = PostgresConfig {
        name: "test_c".to_string(),
        config: tokio_postgres::Config::default()
            .host("127.0.0.1")
            .port(5432)
            .user("postgres")
            .dbname("pagila")
            .to_owned(),
    };

    let connector = PostgresConnector::new(1, postgres_config);
    let tables = connector
        .list_columns(vec![TableIdentifier::from_table_name("users".to_string())])
        .unwrap();
    thread::spawn(move || -> Result<(), ConnectorError> { connector.start(&ingestor, tables) });

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
