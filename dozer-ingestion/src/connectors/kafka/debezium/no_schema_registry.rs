use crate::connectors::kafka::debezium::schema::map_schema;
use crate::connectors::kafka::debezium::stream_consumer::DebeziumMessage;
use crate::connectors::SourceSchema;
use crate::errors::KafkaError::{BytesConvertError, JsonDecodeError, KafkaConnectionError};
use crate::errors::{ConnectorError, KafkaError, KafkaStreamError};
use dozer_types::serde_json;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, DefaultConsumerContext};
use rdkafka::{ClientConfig, Message};

use crate::connectors::CdcType::FullChanges;
use rdkafka::consumer::stream_consumer::StreamConsumer as RdkafkaStreamConsumer;

pub struct NoSchemaRegistry {}

impl NoSchemaRegistry {
    pub async fn get_schema(
        table_names: Option<&[String]>,
        broker: String,
    ) -> Result<Vec<SourceSchema>, ConnectorError> {
        let mut schemas = vec![];
        match table_names {
            None => {}
            Some(tables) => {
                for table in tables {
                    let context = DefaultConsumerContext;

                    let con: RdkafkaStreamConsumer = ClientConfig::new()
                        .set("bootstrap.servers", broker.clone())
                        .set("enable.partition.eof", "false")
                        .set("session.timeout.ms", "6000")
                        .set("enable.auto.commit", "true")
                        .set_log_level(RDKafkaLogLevel::Debug)
                        .create_with_context(context)
                        .map_err(KafkaConnectionError)?;

                    con.subscribe(&[table]).map_err(KafkaConnectionError)?;

                    let m = con.recv().await.map_err(|e| {
                        KafkaError::KafkaStreamError(KafkaStreamError::PollingError(e))
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
                                ConnectorError::KafkaError(KafkaError::KafkaSchemaError(e))
                            })?;

                        schemas.push(SourceSchema::new(mapped_schema, FullChanges));
                    }
                }
            }
        }

        Ok(schemas)
    }
}
