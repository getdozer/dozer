use dozer_ingestion::connectors::connector::Connector;
use dozer_ingestion::connectors::postgres::connector::{PostgresConfig, PostgresConnector};
use dozer_ingestion::connectors::seq_no_resolver::SeqNoResolver;
use dozer_ingestion::connectors::storage::{RocksConfig, RocksStorage, Storage};

use std::sync::{Arc, Mutex};
use std::time::Instant;
use crate::connectors::connector::TableInfo;


fn main() {
    let storage_config = RocksConfig::default();
    let storage_client = Arc::new(Storage::new(storage_config));
    let postgres_config = PostgresConfig {
        name: "test_c".to_string(),
        tables: Some(vec![TableInfo {
            name: "actor".to_string(),
            id: 0,
            columns: None
        }]),
        conn_str: "host=127.0.0.1 port=5432 user=postgres dbname=pagila".to_string(),
    };

    let mut connector = PostgresConnector::new(postgres_config);

    let mut storage_config = RocksConfig::default();
    storage_config.path = "target/seqs".to_string();
    let lsn_storage_client: Arc<RocksStorage> = Arc::new(Storage::new(storage_config));
    let mut seq_resolver = SeqNoResolver::new(Arc::clone(&lsn_storage_client));
    seq_resolver.init();
    let seq_no_resolver = Arc::new(Mutex::new(seq_resolver));

    connector
        .initialize(storage_client, lsn_storage_client, None)
        .unwrap();

    connector.drop_replication_slot_if_exists().unwrap();

    let before = Instant::now();
    const BACKSPACE: char = 8u8 as char;
    let mut iterator = connector.iterator(seq_no_resolver);
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
