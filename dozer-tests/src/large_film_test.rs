use dozer_ingestion::connectors::connector::Connector;
use dozer_ingestion::connectors::postgres::connector::{PostgresConfig, PostgresConnector};
use dozer_ingestion::connectors::storage::{RocksConfig, RocksStorage, Storage};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn print_no_of_keys(before: Arc<Instant>, storage_client: Arc<RocksStorage>) {
    const BACKSPACE: char = 8u8 as char;
    loop {
        let val: String = storage_client.get_estimate_key_count().to_string();
        print!(
            "{}\rCount: {}, Elapsed time: {:.2?}",
            BACKSPACE,
            val,
            before.elapsed(),
        );
        thread::sleep(Duration::from_millis(50));
    }
}

fn main() {
    // ingestion_server::get_server().await.unwrap();

    let storage_config = RocksConfig {
        path: "./db/test/large_film".to_string(),
    };
    let storage_client: Arc<RocksStorage> = Arc::new(Storage::new(storage_config));

    // destroy apth
    storage_client.destroy();

    let postgres_config = PostgresConfig {
        name: "test_c".to_string(),
        tables: None,
        conn_str: "host=127.0.0.1 port=5432 user=postgres dbname=large_film".to_string(),
    };
    let mut connector = PostgresConnector::new(postgres_config);
    let client1 = Arc::clone(&storage_client);
    let before = Arc::new(Instant::now());
    let before1 = Arc::clone(&before);
    thread::spawn(|| {
        print_no_of_keys(before1, client1);
    });

    connector.initialize(Arc::clone(&storage_client)).unwrap();

    let val: String = storage_client.get_estimate_key_count().to_string();
    print!(
        "\nCount: {}, Elapsed time: {:.2?}, Initial Snapshot completed",
        val,
        before.elapsed()
    );
    thread::sleep(Duration::from_millis(50));

    // For testing purposes
    connector.drop_replication_slot();
    for msg in connector.start().next() {
        println!("main function: {:?}", msg);
        let id: i32 = msg.try_get(0).unwrap();
        println!("{:?}", id);
    }
}
