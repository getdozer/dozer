use std::sync::Arc;

use dozer_core::dag::{
    dag::PortHandle,
    node::{ChannelForwarder, Source},
};
use dozer_ingestion::connectors::{
    connector::{self, Connector},
    postgres::connector::{PostgresConfig, PostgresConnector},
    storage::{RocksConfig, RocksStorage, Storage},
};

pub struct PgSource {
    storage_config: RocksConfig,
    pg_config: PostgresConfig,
}

impl PgSource {
    pub fn new(storage_config: RocksConfig, pg_config: PostgresConfig) -> Self {
        Self {
            storage_config,
            pg_config,
        }
    }
}

impl Source for PgSource {
    fn get_output_ports(&self) -> Option<Vec<PortHandle>> {
        None
    }
    fn init(&self) -> Result<(), String> {
        Ok(())
    }

    fn start(&self, fw: &ChannelForwarder) -> Result<(), String> {
        let storage_client = Arc::new(RocksStorage::new(self.storage_config.clone()));

        let mut connector = PostgresConnector::new(self.pg_config.clone());

        connector.initialize(storage_client).unwrap();
        connector.drop_replication_slot_if_exists();

        let mut iterator = connector.iterator();

        loop {
            let _msg = iterator.next().unwrap();
            fw.send(_msg, None).unwrap();
        }
    }
}
