use std::sync::Arc;

use crate::connectors::Connector;
use crate::ingestion::Ingestor;
use crate::{connectors::TableInfo, errors::ConnectorError};
use dozer_types::ingestion_types::KafkaConfig;

use dozer_types::parking_lot::RwLock;

use tokio::runtime::Runtime;

use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

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
        _table_names: Option<Vec<String>>,
    ) -> Result<Vec<(String, dozer_types::types::Schema)>, ConnectorError> {
        Ok(vec![])
    }

    fn get_tables(&self) -> Result<Vec<TableInfo>, ConnectorError> {
        // Ok(vec![TableInfo {
        //     name: "log".to_string(),
        //     id: 1,
        //     columns: Some(helper::get_columns()),
        // }])
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
        Runtime::new().unwrap().block_on(async {
            run(broker, topic, ingestor, connector_id, self.tables.clone()).await
        })
    }

    fn stop(&self) {}

    fn test_connection(&self) -> Result<(), ConnectorError> {
        todo!()
    }

    fn validate(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

async fn run(
    broker: String,
    topic: String,
    ingestor: Arc<RwLock<Ingestor>>,
    connector_id: u64,
    tables: Option<Vec<TableInfo>>,
) -> Result<(), ConnectorError> {
    let con = Consumer::from_hosts(vec![broker])
        .with_topic(topic)
        .with_fallback_offset(FetchOffset::Earliest)
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()
        .map_err(DebeziumConnectionError)?;

    let table_name = tables.unwrap().get(0).unwrap().name.clone();
    let consumer = DebeziumStreamConsumer::default();
    consumer.run(con, ingestor, connector_id, table_name)
}
