use crate::connectors::postgres::schema_helper::SchemaHelper;

use crate::connectors::postgres::connection::validator::validate_connection;
use crate::connectors::postgres::iterator::PostgresIterator;
use crate::connectors::{Connector, TableInfo};
use crate::errors::{ConnectorError, PostgresConnectorError};
use crate::ingestion::Ingestor;
use dozer_types::log::debug;
use dozer_types::parking_lot::RwLock;
use dozer_types::types::Schema;
use postgres::Client;
use postgres_types::PgLsn;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio_postgres::config::ReplicationMode;
use tokio_postgres::Config;

use super::connection::helper;

#[derive(Clone, Debug)]
pub struct PostgresConfig {
    pub name: String,
    pub tables: Option<Vec<TableInfo>>,
    pub config: Config,
}

pub struct PostgresConnector {
    pub id: u64,
    name: String,
    tables: Option<Vec<TableInfo>>,
    ingestor: Option<Arc<RwLock<Ingestor>>>,
    replication_conn_config: Config,
    conn_config: Config,
}

#[derive(Debug)]
pub struct ReplicationSlotInfo {
    pub name: String,
    pub start_lsn: PgLsn,
}

impl PostgresConnector {
    pub fn new(id: u64, config: PostgresConfig) -> PostgresConnector {
        let mut replication_conn_config = config.config.clone();
        replication_conn_config.replication_mode(ReplicationMode::Logical);

        // conn_str - replication_conn_config
        // conn_str_plain- conn_config

        PostgresConnector {
            id,
            name: config.name,
            conn_config: config.config,
            replication_conn_config,
            tables: config.tables,
            ingestor: None,
        }
    }
}

impl Connector for PostgresConnector {
    fn get_tables(&self) -> Result<Vec<TableInfo>, ConnectorError> {
        let mut helper = SchemaHelper {
            conn_config: self.conn_config.clone(),
        };
        let result_vec = helper.get_tables(None)?;
        Ok(result_vec)
    }

    fn get_schemas(
        &self,
        table_names: Option<Vec<String>>,
    ) -> Result<Vec<(String, Schema)>, ConnectorError> {
        let mut helper = SchemaHelper {
            conn_config: self.conn_config.clone(),
        };
        helper.get_schemas(table_names)
    }

    fn initialize(
        &mut self,
        ingestor: Arc<RwLock<Ingestor>>,
        tables: Option<Vec<TableInfo>>,
    ) -> Result<(), ConnectorError> {
        let client = helper::connect(self.replication_conn_config.clone())?;
        self.tables = tables;
        self.create_publication(client)?;
        self.ingestor = Some(ingestor);
        Ok(())
    }
    fn start(&self) -> Result<(), ConnectorError> {
        let iterator = PostgresIterator::new(
            self.id,
            self.get_publication_name(),
            self.get_slot_name(),
            self.tables.to_owned(),
            self.replication_conn_config.clone(),
            self.ingestor
                .as_ref()
                .map_or(Err(ConnectorError::InitializationError), Ok)?
                .clone(),
            self.conn_config.clone(),
        );
        iterator.start()
    }

    fn stop(&self) {}

    fn test_connection(&self) -> Result<(), ConnectorError> {
        helper::connect(self.replication_conn_config.clone())?;
        Ok(())
    }

    fn validate(&self) -> Result<(), ConnectorError> {
        validate_connection(self.conn_config.clone(), self.tables.clone(), None)?;
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
