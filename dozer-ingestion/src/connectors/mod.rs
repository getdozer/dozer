pub mod postgres;

use crate::connectors::postgres::connector::{PostgresConfig, PostgresConnector};
use crate::errors::ConnectorError;
use crate::ingestion::Ingestor;
use dozer_types::log::debug;
use dozer_types::models::connection::Authentication::PostgresAuthentication;
use dozer_types::models::connection::Connection;
use dozer_types::serde;
use dozer_types::serde::{Deserialize, Serialize};
use dozer_types::types::Schema;
use std::sync::{Arc, RwLock};

// use super::{seq_no_resolver::SeqNoResolver, storage::RocksStorage};
pub trait Connector: Send + Sync {
    fn get_schema(&self, name: String) -> Result<Schema, ConnectorError>;
    fn get_all_schema(&self) -> Result<Vec<(String, Schema)>, ConnectorError>;
    fn get_tables(&self) -> Result<Vec<TableInfo>, ConnectorError>;
    fn test_connection(&self) -> Result<(), ConnectorError>;
    fn initialize(
        &mut self,
        ingestor: Arc<RwLock<Ingestor>>,
        tables: Option<Vec<TableInfo>>,
    ) -> Result<(), ConnectorError>;
    fn stop(&self);
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(crate = "self::serde")]
pub struct TableInfo {
    pub name: String,
    pub id: u32,
    pub columns: Option<Vec<String>>,
}

pub fn get_connector(connection: Connection) -> Box<dyn Connector> {
    match connection.authentication {
        PostgresAuthentication {
            user,
            password: _,
            host,
            port,
            database,
        } => {
            let postgres_config = PostgresConfig {
                name: connection.name,
                tables: None,
                conn_str: format!(
                    "host={} port={} user={} dbname={}",
                    host, port, user, database
                ),
            };
            debug!("Connecting to postgres database - {}", database);
            Box::new(PostgresConnector::new(
                connection.id.unwrap().parse().unwrap(),
                postgres_config,
            ))
        }
    }
}
