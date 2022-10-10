use connectors::connector::Connector;
use connectors::postgres::connector::{PostgresConfig, PostgresConnector};

mod connectors;
use crate::connectors::storage::{RocksConfig, Storage};
use std::sync::Arc;
use std::sync::mpsc::channel;
use std::thread;

use std::time::Instant;
use crossbeam::channel::unbounded;



fn main() {
    let storage_config = RocksConfig::default();
    let storage_client = Arc::new(Storage::new(storage_config));
    let postgres_configs = vec!(
        PostgresConfig {
            name: "test_c".to_string(),
            tables: Some(vec![("actor".to_string(), 0)]),
            // tables: None,
            conn_str: "host=127.0.0.1 port=5432 user=postgres dbname=pagila".to_string(),
            // conn_str: "host=127.0.0.1 port=5432 user=postgres dbname=large_film".to_string(),
        },
        PostgresConfig {
            name: "test_d".to_string(),
            tables: Some(vec![("film".to_string(), 0)]),
            // tables: None,
            conn_str: "host=127.0.0.1 port=5432 user=postgres dbname=large_film".to_string(),
            // conn_str: "host=127.0.0.1 port=5432 user=postgres dbname=large_film".to_string(),
        }
    );

    for postgres_config in postgres_configs {
        let client = Arc::clone(&storage_client);
        let fw = Arc::clone(&forwarder);
        thread::spawn(move || {
            let mut connector = PostgresConnector::new(postgres_config);

            connector.initialize(storage_client, None).unwrap();

            connector.drop_replication_slot_if_exists().unwrap();

            // let ingestor = Ingestor::new(storage_client);

            let before = Instant::now();
            const BACKSPACE: char = 8u8 as char;
            let mut iterator = connector.iterator();
            let mut i = 0;
            loop {
                let _msg = iterator.next().unwrap();
                // println!("{:?}", _msg);
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
        });
    }

    loop {
        let msg = receiver.iter().next().unwrap();
        if msg % 100 == 0 {
            print!(
                "Count: {}",
                msg
            );
        }
    }

    println!("ABC: {:?}", receiver.recv().unwrap());
}
