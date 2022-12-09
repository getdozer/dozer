pub mod ethereum;
pub mod events;
pub mod kafka;
pub mod postgres;

use crate::connectors::postgres::connection::helper::map_connection_config;

use crate::connectors::kafka::connector::KafkaConnector;
use crate::connectors::postgres::connector::{PostgresConfig, PostgresConnector};
use crate::errors::ConnectorError;
use crate::ingestion::Ingestor;
use dozer_types::log::debug;
use dozer_types::models::connection::Authentication;
use dozer_types::models::connection::Connection;
use dozer_types::parking_lot::RwLock;
use dozer_types::serde;
use dozer_types::serde::{Deserialize, Serialize};
use dozer_types::types::Schema;
use std::sync::Arc;
pub mod snowflake;
use self::{ethereum::connector::EthConnector, events::connector::EventsConnector};
use crate::connectors::snowflake::connector::SnowflakeConnector;
// use super::{seq_no_resolver::SeqNoResolver, storage::RocksStorage};
pub trait Connector: Send + Sync {
    fn get_schemas(
        &self,
        table_names: Option<Vec<TableInfo>>,
    ) -> Result<Vec<(String, Schema)>, ConnectorError>;
    fn get_tables(&self) -> Result<Vec<TableInfo>, ConnectorError>;
    fn test_connection(&self) -> Result<(), ConnectorError>;
    fn initialize(
        &mut self,
        ingestor: Arc<RwLock<Ingestor>>,
        tables: Option<Vec<TableInfo>>,
    ) -> Result<(), ConnectorError>;
    fn start(&self) -> Result<(), ConnectorError>;
    fn stop(&self);
    fn validate(&self) -> Result<(), ConnectorError>;
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(crate = "self::serde")]
pub struct TableInfo {
    pub name: String,
    pub id: u32,
    pub columns: Option<Vec<String>>,
}

pub fn get_connector(connection: Connection) -> Result<Box<dyn Connector>, ConnectorError> {
    let authentication = connection.authentication.unwrap_or_default();
    match authentication {
        Authentication::Postgres(_) => {
            let config = map_connection_config(&authentication)?;
            let postgres_config = PostgresConfig {
                name: connection.name.clone(),
                tables: None,
                config,
            };

            if let Some(dbname) = postgres_config.config.get_dbname() {
                debug!("Connecting to postgres database - {}", dbname.to_string());
            }
            Ok(Box::new(PostgresConnector::new(1, postgres_config)))
        }
        Authentication::Ethereum(eth_config) => Ok(Box::new(EthConnector::new(2, eth_config))),
        Authentication::Events(_) => Ok(Box::new(EventsConnector::new(3, connection.name))),
        Authentication::Snowflake(snowflake) => {
            let snowflake_config = snowflake;

            Ok(Box::new(SnowflakeConnector::new(4, snowflake_config)))
        }
        Authentication::Kafka(kafka_config) => Ok(Box::new(KafkaConnector::new(5, kafka_config))),
    }
}
