use dozer_ingestion::connectors::postgres::connector::{PostgresConfig, PostgresConnector};
use dozer_ingestion::connectors::{Connector, TableInfo};
use dozer_ingestion::ingestion::{IngestionConfig, Ingestor};
use dozer_types::log::info;
use dozer_types::log4rs;
use std::thread;
use std::time::Instant;

fn main() -> Result<(), ConnectorError> {
    log4rs::init_file("log4rs.yaml", Default::default())
        .unwrap_or_else(|_e| panic!("Unable to find log4rs config file"));

    let (ingestor, mut iterator) = Ingestor::initialize_channel(IngestionConfig::default());
    let postgres_config = PostgresConfig {
        name: "test_c".to_string(),
        tables: Some(vec![TableInfo {
            name: "users".to_string(),
            id: 0,
            columns: None,
        }]),
        conn_str: "host=127.0.0.1 port=5432 user=postgres dbname=users".to_string(),
    };

    let t = thread::spawn(move || {
        let mut connector = PostgresConnector::new(1, postgres_config);
        connector.initialize(ingestor, None)?;
        connector.start()?;
    });

    let before = Instant::now();
    const BACKSPACE: char = 8u8 as char;
    let mut i = 0;
    loop {
        let _msg = iterator.next().unwrap();
        if i % 100 == 0 {
            info!(
                "{}\rCount: {}, Elapsed time: {:.2?}",
                BACKSPACE,
                i,
                before.elapsed(),
            );
        }
        i += 1;
    }

    t.join().unwrap();
    Ok(())
}
