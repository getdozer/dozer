use dozer_ingestion::connectors::connector::{Connector, TableInfo};
use dozer_ingestion::connectors::postgres::connector::{PostgresConfig, PostgresConnector};
use dozer_ingestion::connectors::seq_no_resolver::SeqNoResolver;
use dozer_ingestion::connectors::storage::{RocksConfig, Storage};
use log::debug;

use std::sync::{Arc, Mutex};
use std::time::Instant;

fn main() {
    log4rs::init_file("log4rs.yaml", Default::default())
        .unwrap_or_else(|_e| panic!("Unable to find log4rs config file"));

    let storage_config = RocksConfig::default();
    let storage_client = Arc::new(Storage::new(storage_config));
    let postgres_config = PostgresConfig {
        name: "test_c".to_string(),
        tables: Some(vec![TableInfo {
            name: "actor".to_string(),
            id: 0,
            columns: None,
        }]),
        conn_str: "host=127.0.0.1 port=5432 user=postgres dbname=pagila".to_string(),
    };

    let mut connector = PostgresConnector::new(1, postgres_config);
    let mut seq_resolver = SeqNoResolver::new(Arc::clone(&storage_client));
    seq_resolver.init();
    let seq_no_resolver = Arc::new(Mutex::new(seq_resolver));

    connector.initialize(storage_client, None).unwrap();

    let before = Instant::now();
    const BACKSPACE: char = 8u8 as char;
    let mut iterator = connector.iterator(seq_no_resolver);
    let mut i = 0;
    loop {
        let _msg = iterator.next().unwrap();
        if i % 1000 == 0 {
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
