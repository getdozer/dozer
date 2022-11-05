use crate::connectors::postgres::iterator::PostgresIterator;
use crate::connectors::postgres::schema_helper::SchemaHelper;
use crate::connectors::{Connector, TableInfo};
use crate::ingestion::Ingestor;
use dozer_types::errors::connector::{ConnectorError, PostgresConnectorError};
use dozer_types::log::debug;
use dozer_types::types::Schema;
use postgres::Client;
use std::sync::{Arc, RwLock};

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
        }
    }
}

impl Connector for PostgresConnector {
    fn get_tables(&self) -> Result<Vec<TableInfo>, ConnectorError> {
        let mut helper = SchemaHelper {
            conn_str: self.conn_str_plain.clone(),
        };
        let result_vec = helper.get_tables()?;
        Ok(result_vec)
    }
    fn get_schema(&self, name: String) -> Result<Schema, ConnectorError> {
        let result_vec = self.get_all_schema()?;
        let result = result_vec
            .iter()
            .find(|&el| el.0 == name)
            .map(|v| v.to_owned().1);
        match result {
            Some(schema) => Ok(schema),
            None => Err(ConnectorError::TableNotFound(name)),
        }
    }

    fn get_all_schema(&self) -> Result<Vec<(String, Schema)>, ConnectorError> {
        let mut helper = SchemaHelper {
            conn_str: self.conn_str_plain.clone(),
        };
        helper.get_schema()
    }

    fn initialize(
        &mut self,
        ingestor: Arc<RwLock<Ingestor>>,
        tables: Option<Vec<TableInfo>>,
    ) -> Result<(), ConnectorError> {
        let client = helper::connect(self.conn_str.clone())?;
        self.tables = tables;
        self.create_publication(client)?;
        let iterator = PostgresIterator::new(
            self.id,
            self.get_publication_name(),
            self.get_slot_name(),
            self.tables.to_owned(),
            self.conn_str.clone(),
            self.conn_str_plain.clone(),
            ingestor,
        );

        let _join_handle = iterator.start().unwrap();
        Ok(())
    }

    fn stop(&self) {}

    fn test_connection(&self) -> Result<(), ConnectorError> {
        helper::connect(self.conn_str.clone())?;
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

    fn create_publication(&self, mut client: Client) -> Result<(), ConnectorError> {
        let publication_name = self.get_publication_name();
        let table_str: String = match self.tables.as_ref() {
            None => "ALL TABLES".to_string(),
            Some(arr) => {
                let table_names: Vec<String> = arr.iter().map(|t| t.name.clone()).collect();
                format!("TABLE {}", table_names.join(" "))
            }
        };

        client
            .simple_query(format!("DROP PUBLICATION IF EXISTS {}", publication_name).as_str())
            .map_err(|e| {
                debug!("failed to drop publication {}", e.to_string());
                PostgresConnectorError::DropPublicationError
            })?;

        client
            .simple_query(
                format!("CREATE PUBLICATION {} FOR {}", publication_name, table_str).as_str(),
            )
            .map_err(|e| {
                debug!("failed to create publication {}", e.to_string());
                PostgresConnectorError::CreatePublicationError
            })?;
        Ok(())
    }
}
