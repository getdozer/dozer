pub mod ethereum;
pub mod events;
pub mod kafka;
pub mod object_store;
pub mod postgres;

use crate::connectors::postgres::connection::helper::map_connection_config;
use std::collections::HashMap;

use crate::connectors::kafka::connector::KafkaConnector;
use crate::connectors::postgres::connector::{PostgresConfig, PostgresConnector};
use crate::errors::ConnectorError;
use crate::ingestion::Ingestor;
use dozer_types::log::debug;
use dozer_types::models::connection::Authentication;
use dozer_types::models::connection::Connection;

use crate::connectors::object_store::connector::ObjectStoreConnector;
use dozer_types::prettytable::Table;
use dozer_types::serde;
use dozer_types::serde::{Deserialize, Serialize};
use dozer_types::types::SchemaWithChangesType;

pub mod snowflake;

use self::{ethereum::connector::EthConnector, events::connector::EventsConnector};
use crate::connectors::snowflake::connector::SnowflakeConnector;

pub type ValidationResults = HashMap<String, Vec<(Option<String>, Result<(), ConnectorError>)>>;

pub trait Connector: Send + Sync {
    fn validate(&self, tables: Option<Vec<TableInfo>>) -> Result<(), ConnectorError>;
    fn validate_schemas(&self, tables: &[TableInfo]) -> Result<ValidationResults, ConnectorError>;

    fn get_schemas(
        &self,
        table_names: Option<Vec<TableInfo>>,
    ) -> Result<Vec<SchemaWithChangesType>, ConnectorError>;
    fn start(
        &self,
        from_seq: Option<(u64, u64)>,
        ingestor: &Ingestor,
        tables: Option<Vec<TableInfo>>,
    ) -> Result<(), ConnectorError>;
    fn get_tables(&self, tables: Option<&[TableInfo]>) -> Result<Vec<TableInfo>, ConnectorError>;
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(crate = "self::serde")]
pub struct TableInfo {
    pub name: String,
    pub table_name: String,
    pub id: u32,
    pub columns: Option<Vec<ColumnInfo>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(crate = "self::serde")]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: Option<String>,
}

pub fn get_connector(connection: Connection) -> Result<Box<dyn Connector>, ConnectorError> {
    let authentication = connection.authentication.unwrap_or_default();
    match authentication {
        Authentication::Postgres(_) => {
            let config = map_connection_config(&authentication)?;
            let postgres_config = PostgresConfig {
                name: connection.name,
                tables: None,
                config,
            };

            if let Some(dbname) = postgres_config.config.get_dbname() {
                debug!("Connecting to postgres database - {}", dbname.to_string());
            }
            Ok(Box::new(PostgresConnector::new(1, postgres_config)))
        }
        Authentication::Ethereum(eth_config) => {
            Ok(Box::new(EthConnector::new(2, eth_config, connection.name)))
        }
        Authentication::Events(_) => Ok(Box::new(EventsConnector::new(3, connection.name))),
        Authentication::Snowflake(snowflake) => {
            let snowflake_config = snowflake;

            Ok(Box::new(SnowflakeConnector::new(
                connection.name,
                snowflake_config,
            )))
        }
        Authentication::Kafka(kafka_config) => Ok(Box::new(KafkaConnector::new(5, kafka_config))),
        Authentication::S3Storage(object_store_config) => {
            Ok(Box::new(ObjectStoreConnector::new(5, object_store_config)))
        }
        Authentication::LocalStorage(object_store_config) => {
            Ok(Box::new(ObjectStoreConnector::new(5, object_store_config)))
        }
    }
}

pub fn get_connector_info_table(connection: &Connection) -> Option<Table> {
    match &connection.authentication {
        Some(Authentication::Postgres(config)) => Some(config.convert_to_table()),
        Some(Authentication::Ethereum(config)) => Some(config.convert_to_table()),
        Some(Authentication::Snowflake(config)) => Some(config.convert_to_table()),
        Some(Authentication::Kafka(config)) => Some(config.convert_to_table()),
        Some(Authentication::S3Storage(config)) => Some(config.convert_to_table()),
        Some(Authentication::LocalStorage(config)) => Some(config.convert_to_table()),
        _ => None,
    }
}
