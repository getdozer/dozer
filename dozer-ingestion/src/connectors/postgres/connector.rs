use crate::connectors::connector;
use crate::connectors::postgres::iterator::PostgresIterator;
use crate::connectors::postgres::schema_helper::SchemaHelper;
use crate::connectors::postgres::snapshotter::PostgresSnapshotter;
use crate::connectors::postgres::xlog_mapper::XlogMapper;
use crate::connectors::storage::RocksStorage;
use async_trait::async_trait;
use connector::Connector;
use dozer_shared::types::TableInfo;
use postgres::Client;
use std::sync::Arc;

use super::helper;

pub struct PostgresConfig {
    pub name: String,
    pub tables: Option<Vec<String>>,
    pub conn_str: String,
}

pub struct PostgresConnector {
    name: String,
    conn_str: String,
    conn_str_plain: String,
    tables: Option<Vec<String>>,
    storage_client: Option<Arc<RocksStorage>>,
}

impl Connector<PostgresConfig, postgres::Client, postgres::Error> for PostgresConnector {
    fn new(config: PostgresConfig) -> PostgresConnector {
        let mut conn_str = config.conn_str.to_owned();
        conn_str.push_str(" replication=database");

        PostgresConnector {
            name: config.name,
            conn_str_plain: config.conn_str,
            conn_str,
            tables: config.tables,
            storage_client: None,
        }
    }

    fn initialize(&mut self, storage_client: Arc<RocksStorage>) -> Result<(), postgres::Error> {
        self.storage_client = Some(storage_client);
        let client = helper::connect(self.conn_str.clone())?;
        self.create_publication(client).unwrap();
        Ok(())
    }

    fn test_connection(&self) -> Result<(), postgres::Error> {
        helper::connect(self.conn_str.clone())?;
        Ok(())
    }

    fn get_schema(&self) -> Vec<TableInfo> {
        let mut helper = SchemaHelper {
            conn_str: self.conn_str_plain.clone(),
        };
        helper.get_schema()
    }

    fn start(&mut self) -> PostgresIterator {
        let stream = PostgresIterator::new(
            self.get_publication_name(),
            self.get_slot_name(),
            self.tables.to_owned(),
            self.conn_str.clone(),
            self.conn_str_plain.clone(),
            Arc::clone(&self.storage_client.as_ref().unwrap()),
        );
        stream
    }

    fn stop(&self) {}
}

impl PostgresConnector {
    fn get_publication_name(&self) -> String {
        format!("dozer_publication_{}", self.name)
    }

    fn get_slot_name(&self) -> String {
        format!("dozer_slot_{}", self.name)
    }

    fn create_publication(&self, mut client: Client) -> Result<(), postgres::Error> {
        let publication_name = self.get_publication_name();
        let table_str: String = match self.tables.as_ref() {
            None => "ALL TABLES".to_string(),
            Some(arr) => format!("TABLE {}", arr.join(" ")).to_string(),
        };

        client.simple_query(format!("DROP PUBLICATION IF EXISTS {}", publication_name).as_str())?;

        client.simple_query(
            format!("CREATE PUBLICATION {} FOR {}", publication_name, table_str).as_str(),
        )?;
        Ok(())
    }

    pub fn drop_replication_slot(&self) {
        let slot = self.get_slot_name();
        let mut client = helper::connect(self.conn_str.clone()).unwrap();
        client
            .simple_query(format!("select pg_drop_replication_slot('{}');", slot).as_ref())
            .unwrap();
    }
}
