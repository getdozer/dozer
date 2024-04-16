#[cfg(feature = "ethereum")]
use dozer_ingestion_connector::dozer_types::models::ingestion_types::EthProviderConfig;
use dozer_ingestion_connector::dozer_types::{
    event::EventHub,
    log::debug,
    models::{
        connection::{Connection, ConnectionConfig},
        ingestion_types::default_grpc_adapter,
    },
    prettytable::Table,
};
#[cfg(feature = "datafusion")]
use dozer_ingestion_deltalake::DeltaLakeConnector;
#[cfg(feature = "ethereum")]
use dozer_ingestion_ethereum::{EthLogConnector, EthTraceConnector};
use dozer_ingestion_grpc::{connector::GrpcConnector, ArrowAdapter, DefaultAdapter};
#[cfg(feature = "javascript")]
use dozer_ingestion_javascript::JavaScriptConnector;
#[cfg(feature = "kafka")]
use dozer_ingestion_kafka::connector::KafkaConnector;
#[cfg(feature = "mongodb")]
use dozer_ingestion_mongodb::MongodbConnector;
use dozer_ingestion_mysql::connector::{mysql_connection_opts_from_url, MySQLConnector};
#[cfg(feature = "datafusion")]
use dozer_ingestion_object_store::connector::ObjectStoreConnector;
use dozer_ingestion_postgres::{
    connection::helper::map_connection_config,
    connector::{PostgresConfig, PostgresConnector},
};
#[cfg(feature = "snowflake")]
use dozer_ingestion_snowflake::connector::SnowflakeConnector;
use dozer_ingestion_webhook::connector::WebhookConnector;
use errors::ConnectorError;
use std::sync::Arc;
use tokio::runtime::Runtime;

pub mod errors;
pub use dozer_ingestion_connector::*;

const DEFAULT_POSTGRES_SNAPSHOT_BATCH_SIZE: u32 = 100_000;

#[allow(unused_variables)]
pub fn get_connector(
    runtime: Arc<Runtime>,
    event_hub: EventHub,
    connection: Connection,
    state: Option<Vec<u8>>,
) -> Result<Box<dyn Connector>, ConnectorError> {
    let config = connection.config;
    match config.clone() {
        ConnectionConfig::Postgres(c) => {
            let config = map_connection_config(&config)?;
            let postgres_config = PostgresConfig {
                name: connection.name,
                config,
                schema: c.schema,
                batch_size: c.batch_size.unwrap_or(DEFAULT_POSTGRES_SNAPSHOT_BATCH_SIZE) as usize,
            };

            if let Some(dbname) = postgres_config.config.get_dbname() {
                debug!("Connecting to postgres database - {}", dbname.to_string());
            }
            Ok(Box::new(PostgresConnector::new(postgres_config, state)?))
        }
        #[cfg(feature = "ethereum")]
        ConnectionConfig::Ethereum(eth_config) => match eth_config.provider {
            EthProviderConfig::Log(log_config) => {
                Ok(Box::new(EthLogConnector::new(log_config, connection.name)))
            }
            EthProviderConfig::Trace(trace_config) => Ok(Box::new(EthTraceConnector::new(
                trace_config,
                connection.name,
            ))),
        },
        #[cfg(not(feature = "ethereum"))]
        ConnectionConfig::Ethereum(_) => Err(ConnectorError::EthereumFeatureNotEnabled),
        ConnectionConfig::Grpc(grpc_config) => {
            match grpc_config
                .adapter
                .clone()
                .unwrap_or_else(default_grpc_adapter)
                .as_str()
            {
                "arrow" => Ok(Box::new(GrpcConnector::<ArrowAdapter>::new(
                    connection.name,
                    grpc_config,
                ))),
                "default" => Ok(Box::new(GrpcConnector::<DefaultAdapter>::new(
                    connection.name,
                    grpc_config,
                ))),
                _ => Err(ConnectorError::UnsupportedGrpcAdapter(
                    connection.name,
                    grpc_config.adapter,
                )),
            }
        }
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
        #[cfg(feature = "datafusion")]
        ConnectionConfig::S3Storage(object_store_config) => {
            Ok(Box::new(ObjectStoreConnector::new(object_store_config)))
        }
        #[cfg(feature = "datafusion")]
        ConnectionConfig::LocalStorage(object_store_config) => {
            Ok(Box::new(ObjectStoreConnector::new(object_store_config)))
        }
        #[cfg(feature = "datafusion")]
        ConnectionConfig::DeltaLake(delta_lake_config) => {
            Ok(Box::new(DeltaLakeConnector::new(delta_lake_config)))
        }
        #[cfg(not(feature = "datafusion"))]
        ConnectionConfig::DeltaLake(_) => Err(ConnectorError::DatafusionFeatureNotEnabled),
        #[cfg(not(feature = "datafusion"))]
        ConnectionConfig::LocalStorage(_) => Err(ConnectorError::DatafusionFeatureNotEnabled),
        #[cfg(not(feature = "datafusion"))]
        ConnectionConfig::S3Storage(_) => Err(ConnectorError::DatafusionFeatureNotEnabled),
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
        ConnectionConfig::Webhook(webhook_config) => {
            Ok(Box::new(WebhookConnector::new(webhook_config)))
        }
        #[cfg(not(feature = "javascript"))]
        ConnectionConfig::JavaScript(_) => Err(ConnectorError::JavascrtiptFeatureNotEnabled),
        #[cfg(feature = "javascript")]
        ConnectionConfig::JavaScript(javascript_config) => Ok(Box::new(JavaScriptConnector::new(
            runtime,
            javascript_config,
        ))),
        ConnectionConfig::Aerospike(_) => {
            Err(ConnectorError::FeatureNotEnabled("Aerospike".to_string()))
        }
        ConnectionConfig::Oracle(_) => Err(ConnectorError::FeatureNotEnabled("Oracle".to_string())),
    }
}

pub fn get_connector_info_table(connection: &Connection) -> Option<Table> {
    match &connection.config {
        ConnectionConfig::Postgres(config) => match config.replenish() {
            Ok(conf) => Some(conf.convert_to_table()),
            Err(_) => None,
        },
        ConnectionConfig::Ethereum(config) => Some(config.convert_to_table()),
        ConnectionConfig::Snowflake(config) => Some(config.convert_to_table()),
        ConnectionConfig::Kafka(config) => Some(config.convert_to_table()),
        ConnectionConfig::S3Storage(config) => Some(config.convert_to_table()),
        ConnectionConfig::LocalStorage(config) => Some(config.convert_to_table()),
        _ => None,
    }
}
