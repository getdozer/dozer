use crate::connectors::{Connector, ValidationResults};
use crate::ingestion::Ingestor;
use crate::{connectors::TableInfo, errors::ConnectorError};
use dozer_types::ingestion_types::KafkaConfig;

use dozer_types::types::SourceSchema;

use dozer_types::log::{info};

use crate::connectors::kafka::debezium::no_schema_registry::NoSchemaRegistry;
use crate::connectors::kafka::debezium::schema_registry::SchemaRegistry;
use crate::connectors::kafka::debezium::stream_consumer::DebeziumStreamConsumer;
use crate::errors::DebeziumError::{DebeziumConnectionError, TopicNotDefined};

use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, ConsumerContext, DefaultConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::topic_partition_list::TopicPartitionList;
use crate::connectors::kafka::stream_consumer::StreamConsumer as DebeziumConsumer;

// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// This particular context sets up custom callbacks to log rebalancing events.
struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

#[derive(Debug)]
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
    ) -> Result<Vec<SourceSchema>, ConnectorError> {
        self.config.schema_registry_url.clone().map_or(
            NoSchemaRegistry::get_schema(table_names.clone(), self.config.clone()),
            |_| SchemaRegistry::get_schema(table_names, self.config.clone()),
        )
    }

    fn start(
        &self,
        _from_seq: Option<(u64, u64)>,
        ingestor: &Ingestor,
        tables: Vec<TableInfo>,
    ) -> Result<(), ConnectorError> {
        let topic = tables
            .get(0)
            .map_or(Err(TopicNotDefined), |table| Ok(&table.table_name))?;

        let broker = self.config.broker.to_owned();
        run(broker, topic, ingestor)
    }

    fn validate(&self, _tables: Option<Vec<TableInfo>>) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn validate_schemas(&self, _tables: &[TableInfo]) -> Result<ValidationResults, ConnectorError> {
        todo!()
    }

    fn get_tables(&self, tables: Option<&[TableInfo]>) -> Result<Vec<TableInfo>, ConnectorError> {
        self.get_tables_default(tables)
    }

    fn can_start_from(&self, _last_checkpoint: (u64, u64)) -> Result<bool, ConnectorError> {
        Ok(false)
    }
}

fn run(broker: String, topic: &str, ingestor: &Ingestor) -> Result<(), ConnectorError> {
    let consumer = DebeziumStreamConsumer::default();

    let context = DefaultConsumerContext;

    let con: StreamConsumer<DefaultConsumerContext> = ClientConfig::new()
        .set("bootstrap.servers", broker)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .map_err(DebeziumConnectionError)?;

    con.subscribe(&[topic])
        .map_err(DebeziumConnectionError)?;
        
    consumer.run(con, ingestor)
}
