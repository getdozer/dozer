use dozer_ingestion_connector::dozer_types::serde_json;
use dozer_ingestion_connector::{CdcType, SourceSchema};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::stream_consumer::StreamConsumer as RdkafkaStreamConsumer;
use rdkafka::consumer::{Consumer, DefaultConsumerContext};
use rdkafka::{ClientConfig, Message};

use crate::{KafkaError, KafkaStreamError};

use super::schema::map_schema;
use super::stream_consumer::DebeziumMessage;

pub struct NoSchemaRegistry {}

impl NoSchemaRegistry {
    pub async fn get_schema(
        table_names: Option<&[String]>,
        broker: String,
    ) -> Result<Vec<SourceSchema>, KafkaError> {
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
                        .create_with_context(context)?;

                    con.subscribe(&[table])?;

                    let m = con.recv().await.map_err(|e| {
                        KafkaError::KafkaStreamError(KafkaStreamError::PollingError(e))
                    })?;

                    if let (Some(message), Some(key)) = (m.payload(), m.key()) {
                        let value_struct: DebeziumMessage = serde_json::from_str(
                            std::str::from_utf8(message).map_err(KafkaError::BytesConvertError)?,
                        )
                        .map_err(KafkaError::JsonDecodeError)?;
                        let key_struct: DebeziumMessage = serde_json::from_str(
                            std::str::from_utf8(key).map_err(KafkaError::BytesConvertError)?,
                        )
                        .map_err(KafkaError::JsonDecodeError)?;

                        let (mapped_schema, _fields_map) =
                            map_schema(&value_struct.schema, &key_struct.schema)
                                .map_err(KafkaError::KafkaSchemaError)?;

                        schemas.push(SourceSchema::new(mapped_schema, CdcType::FullChanges));
                    }
                }
            }
        }

        Ok(schemas)
    }
}
