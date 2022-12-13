use std::sync::Arc;

use crate::connectors::Connector;
use crate::ingestion::Ingestor;
use crate::{connectors::TableInfo, errors::ConnectorError};
use dozer_types::ingestion_types::KafkaConfig;

use dozer_types::parking_lot::RwLock;

use tokio::runtime::Runtime;

use crate::connectors::kafka::debezium::schema::map_schema;
use dozer_types::serde_json;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

use crate::connectors::kafka::debezium::stream_consumer::{
    DebeziumMessage, DebeziumStreamConsumer,
};
use crate::connectors::kafka::stream_consumer::StreamConsumer;
use crate::errors::DebeziumError::{BytesConvertError, DebeziumConnectionError, JsonDecodeError};
use crate::errors::{DebeziumError, DebeziumStreamError};

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
    ) -> Result<Vec<(String, dozer_types::types::Schema)>, ConnectorError> {
        table_names.map_or(Ok(vec![]), |tables| {
            tables.get(0).map_or(Ok(vec![]), |table| {
                let mut con = Consumer::from_hosts(vec![self.config.broker.clone()])
                    .with_topic(table.name.clone())
                    .with_fallback_offset(FetchOffset::Earliest)
                    .with_offset_storage(GroupOffsetStorage::Kafka)
                    .create()
                    .map_err(DebeziumConnectionError)?;

                let mut schemas = vec![];
                let mss = con.poll().map_err(|e| {
                    DebeziumError::DebeziumStreamError(DebeziumStreamError::PollingError(e))
                })?;

                if !mss.is_empty() {
                    for ms in mss.iter() {
                        for m in ms.messages() {
                            let value_struct: DebeziumMessage = serde_json::from_str(
                                std::str::from_utf8(m.value).map_err(BytesConvertError)?,
                            )
                            .map_err(JsonDecodeError)?;
                            let key_struct: DebeziumMessage = serde_json::from_str(
                                std::str::from_utf8(m.key).map_err(BytesConvertError)?,
                            )
                            .map_err(JsonDecodeError)?;

                            let (mapped_schema, _fields_map) = map_schema(
                                &value_struct.schema,
                                &key_struct.schema,
                            )
                            .map_err(|e| {
                                ConnectorError::DebeziumError(DebeziumError::DebeziumSchemaError(e))
                            })?;

                            schemas.push((table.name.clone(), mapped_schema));
                        }
                    }
                }

                Ok(schemas)
            })
        })
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
