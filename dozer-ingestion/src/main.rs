use connectors::connector::Connector;
use connectors::postgres::connector::{PostgresConfig, PostgresConnector};
use connectors::postgres::iterator::PostgresIterator;
mod connectors;
use crate::connectors::storage::{RocksConfig, Storage};
use std::sync::Arc;

fn main() {
    let storage_config = RocksConfig {
        path: "./db/embedded".to_string(),
    };
    let storage_client = Arc::new(Storage::new(storage_config));

    let postgres_config = PostgresConfig {
        name: "test_c".to_string(),
        tables: Some(vec!["actor".to_string()]),
        conn_str: "host=127.0.0.1 port=5432 user=postgres dbname=pagila".to_string(),
    };
    let mut connector = PostgresConnector::new(postgres_config);

    connector.initialize(storage_client).unwrap();

    // connector.drop_replication_slot().await;
    // For testing purposes

    let mut iterator: PostgresIterator = connector.start();

    // loop {
    //     let msg = iterator.receiver.recv().unwrap();
    //     println!("main function: {:?}", msg);
    // }
    for msg in iterator.next() {
        println!("main function: {:?}", msg);
        let id: i32 = msg.try_get(0).unwrap();
        println!("{:?}", id);
    }
}
