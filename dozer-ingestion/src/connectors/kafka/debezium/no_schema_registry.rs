use crate::connectors::kafka::debezium::schema::map_schema;
use crate::connectors::kafka::debezium::stream_consumer::DebeziumMessage;
use crate::connectors::TableInfo;
use crate::errors::DebeziumError::{BytesConvertError, DebeziumConnectionError, JsonDecodeError};
use crate::errors::{ConnectorError, DebeziumError, DebeziumStreamError};
use dozer_types::ingestion_types::KafkaConfig;
use dozer_types::serde_json;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, DefaultConsumerContext};
use rdkafka::{ClientConfig, Message};

use dozer_types::types::{ReplicationChangesTrackingType, SourceSchema};
use rdkafka::consumer::stream_consumer::StreamConsumer as RdkafkaStreamConsumer;
use tokio::runtime::Runtime;

pub struct NoSchemaRegistry {}

impl NoSchemaRegistry {
    pub fn get_schema(
        table_names: Option<Vec<TableInfo>>,
        config: KafkaConfig,
    ) -> Result<Vec<SourceSchema>, ConnectorError> {
        table_names.map_or(Ok(vec![]), |tables| {
            tables.get(0).map_or(Ok(vec![]), |table| {
                let context = DefaultConsumerContext;

                let con: RdkafkaStreamConsumer<DefaultConsumerContext> = ClientConfig::new()
                    .set("bootstrap.servers", config.broker.clone())
                    .set("enable.partition.eof", "false")
                    .set("session.timeout.ms", "6000")
                    .set("enable.auto.commit", "true")
                    .set_log_level(RDKafkaLogLevel::Debug)
                    .create_with_context(context)
                    .map_err(DebeziumConnectionError)?;

                con.subscribe(&[table.table_name.as_str()])
                    .map_err(DebeziumConnectionError)?;

                Runtime::new().unwrap().block_on(async {
                    let mut schemas = vec![];
                    let m = con.recv().await.map_err(|e| {
                        DebeziumError::DebeziumStreamError(DebeziumStreamError::PollingError(e))
                    })?;

                    if let (Some(message), Some(key)) = (m.payload(), m.key()) {
                        let value_struct: DebeziumMessage = serde_json::from_str(
                            std::str::from_utf8(message).map_err(BytesConvertError)?,
                        )
                        .map_err(JsonDecodeError)?;
                        let key_struct: DebeziumMessage = serde_json::from_str(
                            std::str::from_utf8(key).map_err(BytesConvertError)?,
                        )
                        .map_err(JsonDecodeError)?;

                        let (mapped_schema, _fields_map) =
                            map_schema(&value_struct.schema, &key_struct.schema).map_err(|e| {
                                ConnectorError::DebeziumError(DebeziumError::DebeziumSchemaError(e))
                            })?;

                        schemas.push(SourceSchema::new(
                            table.table_name.clone(),
                            mapped_schema,
                            ReplicationChangesTrackingType::FullChanges,
                        ));
                    }

                    Ok(schemas)
                })
            })
        })
    }
}
