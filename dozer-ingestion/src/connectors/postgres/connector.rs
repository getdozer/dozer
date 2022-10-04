use crate::connectors::connector;
use crate::connectors::postgres::iterator::PostgresIterator;
use crate::connectors::postgres::schema_helper::SchemaHelper;
use crate::connectors::storage::RocksStorage;
use connector::Connector;
use dozer_schema::registry::SchemaRegistryClient;
use dozer_types::types::{OperationEvent, TableInfo};
use postgres::Client;
use std::sync::Arc;

use super::helper;

#[derive(Clone, Debug)]
pub struct PostgresConfig {
    pub name: String,
    pub tables: Option<Vec<(String, u32)>>,
    pub conn_str: String,
}

pub struct PostgresConnector {
    name: String,
    conn_str: String,
    conn_str_plain: String,
    tables: Option<Vec<(String, u32)>>,
    storage_client: Option<Arc<RocksStorage>>,
    schema_client: Option<Arc<SchemaRegistryClient>>,
}
impl PostgresConnector {
    pub fn new(config: PostgresConfig) -> PostgresConnector {
        let mut conn_str = config.conn_str.to_owned();
        conn_str.push_str(" replication=database");

        PostgresConnector {
            name: config.name,
            conn_str,
            conn_str_plain: config.conn_str,
            tables: config.tables,
            storage_client: None,
            schema_client: None,
        }
    }
}

impl Connector for PostgresConnector {
    fn initialize(
        &mut self,
        storage_client: Arc<RocksStorage>,
        schema_client: Arc<SchemaRegistryClient>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let client = helper::connect(self.conn_str.clone())?;
        self.create_publication(client).unwrap();
        self.storage_client = Some(storage_client);
        self.schema_client = Some(schema_client);
        Ok(())
    }

    fn test_connection(&self) -> anyhow::Result<()> {
        helper::connect(self.conn_str.clone()).map_err(|e| Box::new(e))?;
        Ok(())
    }

    fn get_schema(&self) -> anyhow::Result<Vec<TableInfo>> {
        let mut helper = SchemaHelper {
            conn_str: self.conn_str_plain.clone(),
        };
        helper.get_schema()
    }

    fn iterator(&mut self) -> Box<dyn Iterator<Item = OperationEvent> + 'static> {
        let storage_client = self.storage_client.as_ref().unwrap().clone();
        let schema_client = self.schema_client.as_ref().unwrap().clone();
        let iterator = PostgresIterator::new(
            self.get_publication_name(),
            self.get_slot_name(),
            self.tables.to_owned(),
            self.conn_str.clone(),
            self.conn_str_plain.clone(),
            storage_client,
            schema_client,
        );

        let _join_handle = iterator.start().unwrap();
        Box::new(iterator)
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
            Some(arr) => {
                let (table_names, _): (Vec<String>, Vec<_>) = arr.clone().into_iter().unzip();
                format!("TABLE {}", table_names.join(" ")).to_string()
            }
        };

        client.simple_query(format!("DROP PUBLICATION IF EXISTS {}", publication_name).as_str())?;

        client.simple_query(
            format!("CREATE PUBLICATION {} FOR {}", publication_name, table_str).as_str(),
        )?;
        Ok(())
    }

    pub fn drop_replication_slot_if_exists(&self) {
        let slot = self.get_slot_name();
        let mut client = helper::connect(self.conn_str.clone()).unwrap();
        let res =
            client.simple_query(format!("select pg_drop_replication_slot('{}');", slot).as_ref());
        match res {
            Ok(_) => println!("dropped replication slot {}", slot),
            Err(_) => println!("failed to drop replication slot..."),
        }
    }
}
