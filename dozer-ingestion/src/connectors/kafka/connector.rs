use std::sync::Arc;

use crate::connectors::Connector;
use crate::ingestion::Ingestor;
use crate::{connectors::TableInfo, errors::ConnectorError};
use dozer_types::ingestion_types::KafkaConfig;

use dozer_types::parking_lot::RwLock;

use tokio::runtime::Runtime;

use dozer_types::models::source::Source;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use dozer_types::types::ReplicationChangesTrackingType;

use crate::connectors::kafka::debezium::no_schema_registry::NoSchemaRegistry;
use crate::connectors::kafka::debezium::schema_registry::SchemaRegistry;
use crate::connectors::kafka::debezium::stream_consumer::DebeziumStreamConsumer;
use crate::connectors::kafka::stream_consumer::StreamConsumer;
use crate::errors::DebeziumError::DebeziumConnectionError;

pub struct KafkaConnector {
    pub id: u64,
    config: KafkaConfig,
    ingestor: Option<Arc<RwLock<Ingestor>>>,
    tables: Option<Vec<TableInfo>>,
}

impl KafkaConnector {
    pub fn new(id: u64, config: KafkaConfig) -> Self {
        Self {
            id,
            config,
            ingestor: None,
            tables: None,
        }
    }
}

impl Connector for KafkaConnector {
    fn get_schemas(
        &self,
        table_names: Option<Vec<TableInfo>>,
    ) -> Result<Vec<(String, dozer_types::types::Schema, ReplicationChangesTrackingType)>, ConnectorError> {
        self.config.schema_registry_url.clone().map_or(
            NoSchemaRegistry::get_schema(table_names.clone(), self.config.clone()),
            |_| SchemaRegistry::get_schema(table_names, self.config.clone()),
        )
    }

    fn get_tables(&self) -> Result<Vec<TableInfo>, ConnectorError> {
        Ok(vec![])
    }

    fn initialize(
        &mut self,
        ingestor: Arc<RwLock<Ingestor>>,
        tables: Option<Vec<TableInfo>>,
    ) -> Result<(), ConnectorError> {
        self.ingestor = Some(ingestor);
        self.tables = tables;
        Ok(())
    }

    fn start(&self) -> Result<(), ConnectorError> {
        // Start a new thread that interfaces with ETH node
        let topic = self.config.topic.to_owned();
        let broker = self.config.broker.to_owned();
        let connector_id = self.id;
        let ingestor = self
            .ingestor
            .as_ref()
            .map_or(Err(ConnectorError::InitializationError), Ok)?
            .clone();
        Runtime::new()
            .unwrap()
            .block_on(async { run(broker, topic, ingestor, connector_id).await })
    }

    fn stop(&self) {}

    fn test_connection(&self) -> Result<(), ConnectorError> {
        todo!()
    }

    fn validate(&self, _tables: Option<Vec<TableInfo>>) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn get_connection_groups(sources: Vec<Source>) -> Vec<Vec<Source>> {
        sources.iter().map(|s| vec![s.clone()]).collect()
    }
}

async fn run(
    broker: String,
    topic: String,
    ingestor: Arc<RwLock<Ingestor>>,
    connector_id: u64,
) -> Result<(), ConnectorError> {
    let con = Consumer::from_hosts(vec![broker])
        .with_topic(topic)
        .with_fallback_offset(FetchOffset::Earliest)
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()
        .map_err(DebeziumConnectionError)?;

    let consumer = DebeziumStreamConsumer::default();
    consumer.run(con, ingestor, connector_id)
}
