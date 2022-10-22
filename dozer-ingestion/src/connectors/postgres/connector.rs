use crate::connectors::connector::{self, TableInfo};
use crate::connectors::postgres::schema_helper::SchemaHelper;
use crate::connectors::storage::RocksStorage;
use connector::Connector;

use crate::connectors::postgres::iterator::PostgresIterator;
use crate::connectors::seq_no_resolver::SeqNoResolver;
use anyhow::Context;
use dozer_types::log::debug;
use dozer_types::types::{OperationEvent, Schema};
use postgres::Client;
use std::sync::{Arc, Mutex};

use super::helper;

#[derive(Clone, Debug)]
pub struct PostgresConfig {
    pub name: String,
    pub tables: Option<Vec<TableInfo>>,
    pub conn_str: String,
}

pub struct PostgresConnector {
    pub id: u64,
    name: String,
    conn_str: String,
    conn_str_plain: String,
    tables: Option<Vec<TableInfo>>,
    storage_client: Option<Arc<RocksStorage>>,
}
impl PostgresConnector {
    pub fn new(id: u64, config: PostgresConfig) -> PostgresConnector {
        let mut conn_str = config.conn_str.to_owned();
        conn_str.push_str(" replication=database");

        PostgresConnector {
            id,
            name: config.name,
            conn_str,
            conn_str_plain: config.conn_str,
            tables: config.tables,
            storage_client: None,
        }
    }
}

impl Connector for PostgresConnector {
    fn get_tables(&self) -> anyhow::Result<Vec<TableInfo>> {
        let mut helper = SchemaHelper {
            conn_str: self.conn_str_plain.clone(),
        };
        let result_vec = helper.get_tables()?;
        Ok(result_vec)
    }
    fn get_schema(&self, name: String) -> Result<Schema, anyhow::Error> {
        let result_vec = self.get_all_schema()?;
        let result = result_vec
            .iter()
            .find(|&el| el.0 == name)
            .map(|v| v.to_owned().1);
        match result {
            Some(schema) => Ok(schema),
            None => panic!("No schema with input name"),
        }
    }

    fn get_all_schema(&self) -> anyhow::Result<Vec<(String, Schema)>> {
        let mut helper = SchemaHelper {
            conn_str: self.conn_str_plain.clone(),
        };
        let result_vec = helper.get_schema()?;
        Ok(result_vec)
    }

    fn initialize(
        &mut self,
        storage_client: Arc<RocksStorage>,
        tables: Option<Vec<TableInfo>>,
    ) -> anyhow::Result<()> {
        let client = helper::connect(self.conn_str.clone())?;
        self.storage_client = Some(storage_client);
        self.tables = tables;

        self.create_publication(client)
            .context("Failed create publication")?;
        Ok(())
    }

    fn iterator(
        &mut self,
        seq_no_resolver: Arc<Mutex<SeqNoResolver>>,
    ) -> Box<dyn Iterator<Item = OperationEvent> + 'static> {
        let storage_client = self.storage_client.as_ref().unwrap().clone();
        let iterator = PostgresIterator::new(
            self.id,
            self.get_publication_name(),
            self.get_slot_name(),
            self.tables.to_owned(),
            self.conn_str.clone(),
            self.conn_str_plain.clone(),
            storage_client,
        );

        let _join_handle = iterator.start(seq_no_resolver).unwrap();
        Box::new(iterator)
    }

    fn stop(&self) {}

    fn test_connection(&self) -> anyhow::Result<()> {
        helper::connect(self.conn_str.clone()).map_err(Box::new)?;
        Ok(())
    }
}

impl PostgresConnector {
    fn get_publication_name(&self) -> String {
        format!("dozer_publication_{}", self.name)
    }

    fn get_slot_name(&self) -> String {
        format!("dozer_slot_{}", self.name)
    }

    fn create_publication(&self, mut client: Client) -> anyhow::Result<()> {
        let publication_name = self.get_publication_name();
        let table_str: String = match self.tables.as_ref() {
            None => "ALL TABLES".to_string(),
            Some(arr) => {
                let table_names: Vec<String> = arr.iter().map(|t| t.name.clone()).collect();
                format!("TABLE {}", table_names.join(" "))
            }
        };

        client.simple_query(format!("DROP PUBLICATION IF EXISTS {}", publication_name).as_str())?;

        client.simple_query(
            format!("CREATE PUBLICATION {} FOR {}", publication_name, table_str).as_str(),
        )?;
        Ok(())
    }

    pub fn drop_replication_slot_if_exists(&self) -> anyhow::Result<()> {
        let slot = self.get_slot_name();
        let mut client = helper::connect(self.conn_str.clone())?;
        let res =
            client.simple_query(format!("select pg_drop_replication_slot('{}');", slot).as_ref());
        match res {
            Ok(_) => debug!("dropped replication slot {}", slot),
            Err(_) => debug!("failed to drop replication slot..."),
        }
        Ok(())
    }
}
