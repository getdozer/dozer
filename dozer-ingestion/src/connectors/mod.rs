pub mod dozer;
#[cfg(feature = "ethereum")]
pub mod ethereum;
pub mod grpc;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod mysql;
pub mod object_store;
pub mod postgres;

#[cfg(feature = "mongodb")]
pub mod mongodb;

use crate::connectors::postgres::connection::helper::map_connection_config;

use std::fmt::Debug;

#[cfg(feature = "mongodb")]
use self::mongodb::MongodbConnector;
#[cfg(feature = "kafka")]
use crate::connectors::kafka::connector::KafkaConnector;
use crate::connectors::postgres::connector::{PostgresConfig, PostgresConnector};

use crate::errors::ConnectorError;
use crate::ingestion::Ingestor;

use dozer_types::log::debug;
use dozer_types::models::connection::Connection;
use dozer_types::models::connection::ConnectionConfig;
use dozer_types::node::OpIdentifier;
use dozer_types::tonic::async_trait;

use crate::connectors::object_store::connector::ObjectStoreConnector;

use crate::connectors::delta_lake::DeltaLakeConnector;
use dozer_types::prettytable::Table;
use dozer_types::serde;
use dozer_types::serde::{Deserialize, Serialize};
use dozer_types::types::{FieldType, Schema};

pub mod delta_lake;
pub mod snowflake;

use self::dozer::NestedDozerConnector;
#[cfg(feature = "ethereum")]
use self::ethereum::{EthLogConnector, EthTraceConnector};

use self::grpc::connector::GrpcConnector;
use self::grpc::{ArrowAdapter, DefaultAdapter};
use self::mysql::connector::{mysql_connection_opts_from_url, MySQLConnector};
#[cfg(feature = "snowflake")]
use crate::connectors::snowflake::connector::SnowflakeConnector;
use crate::errors::ConnectorError::{MissingConfiguration, WrongConnectionConfiguration};

#[derive(Clone, Copy, Serialize, Deserialize, Debug, Eq, PartialEq, Default)]
#[serde(crate = "dozer_types::serde")]
/// A source table's CDC event type.
pub enum CdcType {
    /// Connector gets old record on delete/update operations.
    FullChanges,
    /// Connector only gets PK of old record on delete/update operations.
    OnlyPK,
    #[default]
    /// Connector cannot get any info about old records. In other words, the table is append-only.
    Nothing,
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(crate = "dozer_types::serde")]
/// A source table's schema and CDC type.
pub struct SourceSchema {
    /// Dozer schema mapped from the source table. Columns are already filtered based on `TableInfo.column_names`.
    pub schema: Schema,
    #[serde(default)]
    /// The source table's CDC type.
    pub cdc_type: CdcType,
}

impl SourceSchema {
    pub fn new(schema: Schema, cdc_type: CdcType) -> Self {
        Self { schema, cdc_type }
    }
}

/// Result of mapping one source table schema to Dozer schema.
pub type SourceSchemaResult = Result<SourceSchema, ConnectorError>;

#[async_trait]
pub trait Connector: Send + Sync + Debug {
    /// Returns all the external types and their corresponding Dozer types.
    /// If the external type is not supported, None should be returned.
    fn types_mapping() -> Vec<(String, Option<FieldType>)>
    where
        Self: Sized;

    /// Validates the connector's connection level properties.
    async fn validate_connection(&self) -> Result<(), ConnectorError>;

    /// Lists all the table names in the connector.
    async fn list_tables(&self) -> Result<Vec<TableIdentifier>, ConnectorError>;

    /// Validates the connector's table level properties for each table.
    async fn validate_tables(&self, tables: &[TableIdentifier]) -> Result<(), ConnectorError>;

    /// Lists all the column names for each table.
    async fn list_columns(
        &self,
        tables: Vec<TableIdentifier>,
    ) -> Result<Vec<TableInfo>, ConnectorError>;

    /// Gets the schema for each table. Only requested columns need to be mapped.
    ///
    /// If this function fails at the connector level, such as a network error, it should return a outer level `Err`.
    /// Otherwise the outer level `Ok` should always contain the same number of elements as `table_infos`.
    ///
    /// If it fails at the table or column level, such as a unsupported data type, one of the elements should be `Err`.
    async fn get_schemas(
        &self,
        table_infos: &[TableInfo],
    ) -> Result<Vec<SourceSchemaResult>, ConnectorError>;

    /// Lists all tables and columns and gets the schema for each table.
    async fn list_all_schemas(
        &self,
    ) -> Result<(Vec<TableInfo>, Vec<SourceSchema>), ConnectorError> {
        let tables = self.list_tables().await?;
        let table_infos = self.list_columns(tables).await?;
        let schemas = self
            .get_schemas(&table_infos)
            .await?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        Ok((table_infos, schemas))
    }

    /// Starts outputting data from `tables` to `ingestor`. This method should never return unless there is an unrecoverable error.
    async fn start(
        &self,
        ingestor: &Ingestor,
        tables: Vec<TableToIngest>,
    ) -> Result<(), ConnectorError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// Unique identifier of a source table. A source table must have a `name`, optionally under a `schema` scope.
pub struct TableIdentifier {
    /// The `schema` scope of the table.
    ///
    /// Connector that supports schema scope must decide on a default schema, that doesn't must assert that `schema.is_none()`.
    pub schema: Option<String>,
    /// The table name, must be unique under the `schema` scope, or global scope if `schema` is `None`.
    pub name: String,
}

impl TableIdentifier {
    pub fn new(schema: Option<String>, name: String) -> Self {
        Self { schema, name }
    }

    pub fn from_table_name(name: String) -> Self {
        Self { schema: None, name }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(crate = "self::serde")]
/// `TableIdentifier` with column names.
pub struct TableInfo {
    /// The `schema` scope of the table.
    pub schema: Option<String>,
    /// The table name, must be unique under the `schema` scope, or global scope if `schema` is `None`.
    pub name: String,
    /// The column names to be mapped.
    pub column_names: Vec<String>,
}

#[derive(Debug, Clone)]
/// `TableInfo` with an optional checkpoint info.
pub struct TableToIngest {
    /// The `schema` scope of the table.
    pub schema: Option<String>,
    /// The table name, must be unique under the `schema` scope, or global scope if `schema` is `None`.
    pub name: String,
    /// The column names to be mapped.
    pub column_names: Vec<String>,
    /// The checkpoint to start after.
    pub checkpoint: Option<OpIdentifier>,
}

impl TableToIngest {
    pub fn from_scratch(table_info: TableInfo) -> Self {
        Self {
            schema: table_info.schema,
            name: table_info.name,
            column_names: table_info.column_names,
            checkpoint: None,
        }
    }
}

pub fn get_connector(connection: Connection) -> Result<Box<dyn Connector>, ConnectorError> {
    let config = connection
        .config
        .ok_or_else(|| ConnectorError::MissingConfiguration(connection.name.clone()))?;
    match config {
        ConnectionConfig::Postgres(_) => {
            let config = map_connection_config(&config)?;
            let postgres_config = PostgresConfig {
                name: connection.name,
                config,
            };

            if let Some(dbname) = postgres_config.config.get_dbname() {
                debug!("Connecting to postgres database - {}", dbname.to_string());
            }
            Ok(Box::new(PostgresConnector::new(postgres_config)))
        }
        #[cfg(feature = "ethereum")]
        ConnectionConfig::Ethereum(eth_config) => match eth_config.provider.unwrap() {
            dozer_types::ingestion_types::EthProviderConfig::Log(log_config) => {
                Ok(Box::new(EthLogConnector::new(log_config, connection.name)))
            }
            dozer_types::ingestion_types::EthProviderConfig::Trace(trace_config) => Ok(Box::new(
                EthTraceConnector::new(trace_config, connection.name),
            )),
        },
        #[cfg(not(feature = "ethereum"))]
        ConnectionConfig::Ethereum(_) => Err(ConnectorError::EthereumFeatureNotEnabled),
        ConnectionConfig::Grpc(grpc_config) => match grpc_config.adapter.as_str() {
            "arrow" => Ok(Box::new(GrpcConnector::<ArrowAdapter>::new(
                connection.name,
                grpc_config,
            )?)),
            "default" => Ok(Box::new(GrpcConnector::<DefaultAdapter>::new(
                connection.name,
                grpc_config,
            )?)),
            _ => Err(ConnectorError::UnsupportedGrpcAdapter(
                connection.name,
                grpc_config.adapter,
            )),
        },
        #[cfg(feature = "snowflake")]
        ConnectionConfig::Snowflake(snowflake) => {
            let snowflake_config = snowflake;

            Ok(Box::new(SnowflakeConnector::new(
                connection.name,
                snowflake_config,
            )))
        }
        #[cfg(not(feature = "snowflake"))]
        ConnectionConfig::Snowflake(_) => Err(ConnectorError::SnowflakeFeatureNotEnabled),
        #[cfg(feature = "kafka")]
        ConnectionConfig::Kafka(kafka_config) => Ok(Box::new(KafkaConnector::new(kafka_config))),
        #[cfg(not(feature = "kafka"))]
        ConnectionConfig::Kafka(_) => Err(ConnectorError::KafkaFeatureNotEnabled),
        ConnectionConfig::S3Storage(object_store_config) => {
            Ok(Box::new(ObjectStoreConnector::new(object_store_config)))
        }
        ConnectionConfig::LocalStorage(object_store_config) => {
            Ok(Box::new(ObjectStoreConnector::new(object_store_config)))
        }
        ConnectionConfig::DeltaLake(delta_lake_config) => {
            Ok(Box::new(DeltaLakeConnector::new(delta_lake_config)))
        }
        #[cfg(feature = "mongodb")]
        ConnectionConfig::MongoDB(mongodb_config) => {
            let connection_string = mongodb_config.connection_string;
            Ok(Box::new(MongodbConnector::new(connection_string)?))
        }
        #[cfg(not(feature = "mongodb"))]
        ConnectionConfig::MongoDB(_) => Err(ConnectorError::MongodbFeatureNotEnabled),
        ConnectionConfig::MySQL(mysql_config) => {
            let opts = mysql_connection_opts_from_url(&mysql_config.url)?;
            Ok(Box::new(MySQLConnector::new(
                mysql_config.url,
                opts,
                mysql_config.server_id,
            )))
        }
        ConnectionConfig::Dozer(dozer_config) => {
            Ok(Box::new(NestedDozerConnector::new(dozer_config)))
        }
    }
}

pub fn get_connector_info_table(connection: &Connection) -> Result<Table, ConnectorError> {
    match &connection.config {
        Some(ConnectionConfig::Postgres(config)) => match config.replenish() {
            Ok(conf) => Ok(conf.convert_to_table()),
            Err(e) => Err(WrongConnectionConfiguration(e)),
        },
        Some(ConnectionConfig::Ethereum(config)) => Ok(config.convert_to_table()),
        Some(ConnectionConfig::Snowflake(config)) => Ok(config.convert_to_table()),
        Some(ConnectionConfig::Kafka(config)) => Ok(config.convert_to_table()),
        Some(ConnectionConfig::S3Storage(config)) => Ok(config.convert_to_table()),
        Some(ConnectionConfig::LocalStorage(config)) => Ok(config.convert_to_table()),
        _ => Err(MissingConfiguration(connection.name.clone())),
    }
}

fn table_name(schema: Option<&str>, name: &str) -> String {
    if let Some(schema) = &schema {
        format!("{}.{}", schema, name)
    } else {
        name.to_string()
    }
}

#[derive(Debug, Clone)]
pub struct ListOrFilterColumns {
    pub schema: Option<String>,
    pub name: String,
    pub columns: Option<Vec<String>>,
}

pub(crate) fn warn_dropped_primary_index(table_name: &str) {
    dozer_types::log::warn!(
        "One or more primary index columns from the source table are \
                    not part of the defined schema for table: '{0}'. \
                    The primary index will therefore not be present in the Dozer table",
        table_name
    );
}
