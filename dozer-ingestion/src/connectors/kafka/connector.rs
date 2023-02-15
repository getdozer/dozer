use crate::connectors::{Connector, ValidationResults};
use crate::ingestion::Ingestor;
use crate::{connectors::TableInfo, errors::ConnectorError};
use dozer_types::ingestion_types::KafkaConfig;

use tokio::runtime::Runtime;

use dozer_types::types::ReplicationChangesTrackingType;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

use crate::connectors::kafka::debezium::no_schema_registry::NoSchemaRegistry;
use crate::connectors::kafka::debezium::schema_registry::SchemaRegistry;
use crate::connectors::kafka::debezium::stream_consumer::DebeziumStreamConsumer;
use crate::connectors::kafka::stream_consumer::StreamConsumer;
use crate::errors::DebeziumError::{DebeziumConnectionError, TopicNotDefined};

pub struct KafkaConnector {
    pub id: u64,
    config: KafkaConfig,
}

impl KafkaConnector {
    pub fn new(id: u64, config: KafkaConfig) -> Self {
        Self { id, config }
    }
}

impl Connector for KafkaConnector {
    fn get_schemas(
        &self,
        table_names: Option<Vec<TableInfo>>,
    ) -> Result<
        Vec<(
            String,
            dozer_types::types::Schema,
            ReplicationChangesTrackingType,
        )>,
        ConnectorError,
    > {
        self.config.schema_registry_url.clone().map_or(
            NoSchemaRegistry::get_schema(table_names.clone(), self.config.clone()),
            |_| SchemaRegistry::get_schema(table_names, self.config.clone()),
        )
    }

    fn start(
        &self,
        _from_seq: Option<(u64, u64)>,
        ingestor: Ingestor,
        tables: Option<Vec<TableInfo>>,
    ) -> Result<(), ConnectorError> {
        // Start a new thread that interfaces with ETH node
        let tables = tables.as_ref().map_or_else(|| Err(TopicNotDefined), Ok)?;
        let topic = tables
            .get(0)
            .map_or(Err(TopicNotDefined), |table| Ok(&table.table_name))?;

        let broker = self.config.broker.to_owned();
        Runtime::new()
            .unwrap()
            .block_on(async { run(broker, topic, ingestor).await })
    }

    fn validate(&self, _tables: Option<Vec<TableInfo>>) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn validate_schemas(&self, _tables: &[TableInfo]) -> Result<ValidationResults, ConnectorError> {
        todo!()
    }

    fn get_tables(&self, _tables: Option<&[TableInfo]>) -> Result<Vec<TableInfo>, ConnectorError> {
        todo!()
    }
}

async fn run(broker: String, topic: &str, ingestor: Ingestor) -> Result<(), ConnectorError> {
    let con = Consumer::from_hosts(vec![broker])
        .with_topic(topic.to_string())
        .with_fallback_offset(FetchOffset::Earliest)
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()
        .map_err(DebeziumConnectionError)?;

    let consumer = DebeziumStreamConsumer::default();
    consumer.run(con, ingestor)
}
