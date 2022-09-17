use connectors::connector::Connector;
use connectors::postgres::connector::{PostgresConfig, PostgresConnector};
mod connectors;
use crate::connectors::storage::{RocksConfig, Storage};
use std::sync::Arc;
use std::time::Instant;

fn main() {
    let storage_config = RocksConfig {
        path: "./db/embedded".to_string(),
    };
    let storage_client = Arc::new(Storage::new(storage_config));

    let postgres_config = PostgresConfig {
        name: "test_c".to_string(),
        // tables: Some(vec!["actor".to_string()]),
        tables: None,
        conn_str: "host=127.0.0.1 port=5432 user=postgres dbname=pagila".to_string(),
        // conn_str: "host=127.0.0.1 port=5432 user=postgres dbname=large_film".to_string(),
    };
    let mut connector = PostgresConnector::new(postgres_config);

    connector.initialize(storage_client).unwrap();

    connector.drop_replication_slot_if_exists();

    // let ingestor = Ingestor::new(storage_client);

    let before = Instant::now();
    const BACKSPACE: char = 8u8 as char;
    let mut iterator = connector.iterator();
    let mut i = 0;
    loop {
        let _msg = iterator.next().unwrap();

        if i % 100 == 0 {
            print!(
                "{}\rCount: {}, Elapsed time: {:.2?}",
                BACKSPACE,
                i,
                before.elapsed(),
            );
        }
        i += 1;
    }
}
