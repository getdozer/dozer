use crate::debezium::mapper::convert_value_to_schema;
use crate::debezium::schema::map_schema;
use crate::stream_consumer::StreamConsumer;
use crate::stream_consumer_helper::{is_network_failure, OffsetsMap, StreamConsumerHelper};
use crate::{KafkaError, KafkaStreamError};

use dozer_ingestion_connector::dozer_types::node::OpIdentifier;
use dozer_ingestion_connector::TableInfo;
use dozer_ingestion_connector::{
    async_trait,
    dozer_types::{
        models::ingestion_types::IngestionMessage,
        serde::{Deserialize, Serialize},
        serde_json,
        serde_json::Value,
        types::{Operation, Record},
    },
    Ingestor,
};
use rdkafka::{ClientConfig, Message};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(crate = "dozer_ingestion_connector::dozer_types::serde")]
#[serde(untagged)]
pub enum DebeziumFieldType {
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    Bool(bool),
    String(String),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(crate = "dozer_ingestion_connector::dozer_types::serde")]
pub struct DebeziumField {
    pub r#type: String,
    pub optional: bool,
    pub default: Option<DebeziumFieldType>,
    pub field: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(crate = "dozer_ingestion_connector::dozer_types::serde")]
pub struct DebeziumSchemaParameters {
    pub scale: Option<String>,
    #[serde(rename(deserialize = "connect.decimal.precision"))]
    pub precision: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(crate = "dozer_ingestion_connector::dozer_types::serde")]
pub struct DebeziumSchemaStruct {
    pub r#type: Value,
    pub fields: Option<Vec<DebeziumSchemaStruct>>,
    pub optional: Option<bool>,
    pub name: Option<String>,
    pub field: Option<String>,
    pub version: Option<i64>,
    pub parameters: Option<DebeziumSchemaParameters>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(crate = "dozer_ingestion_connector::dozer_types::serde")]
pub struct DebeziumPayload {
    pub before: Option<Value>,
    pub after: Option<Value>,
    pub op: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(crate = "dozer_ingestion_connector::dozer_types::serde")]
pub struct DebeziumMessage {
    pub schema: DebeziumSchemaStruct,
    pub payload: DebeziumPayload,
}

#[derive(Default)]
pub struct DebeziumStreamConsumer {}

impl DebeziumStreamConsumer {}

#[async_trait]
impl StreamConsumer for DebeziumStreamConsumer {
    async fn run(
        &self,
        client_config: ClientConfig,
        ingestor: &Ingestor,
        tables: Vec<TableInfo>,
        last_checkpoint: Option<OpIdentifier>,
        _schema_registry_url: &Option<String>,
    ) -> Result<(), KafkaError> {
        assert!(last_checkpoint.is_none());
        let topics: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        let mut con = StreamConsumerHelper::start(&client_config, &topics).await?;
        let mut offsets = OffsetsMap::new();
        loop {
            let m = match con.poll(None).unwrap() {
                Ok(m) => m,
                Err(err) if is_network_failure(&err) => {
                    con = StreamConsumerHelper::resume(&client_config, &topics, &offsets).await?;
                    continue;
                }
                Err(err) => Err(KafkaError::KafkaStreamError(
                    KafkaStreamError::PollingError(err),
                ))?,
            };
            StreamConsumerHelper::update_offsets(&mut offsets, &m);

            if let (Some(message), Some(key)) = (m.payload(), m.key()) {
                let mut value_struct: DebeziumMessage = serde_json::from_str(
                    std::str::from_utf8(message).map_err(KafkaError::BytesConvertError)?,
                )
                .map_err(KafkaError::JsonDecodeError)?;
                let key_struct: DebeziumMessage = serde_json::from_str(
                    std::str::from_utf8(key).map_err(KafkaError::BytesConvertError)?,
                )
                .map_err(KafkaError::JsonDecodeError)?;

                let (schema, fields_map) = map_schema(&value_struct.schema, &key_struct.schema)
                    .map_err(KafkaError::KafkaSchemaError)?;

                // When update happens before is null.
                // If PK value changes, then debezium creates two events - delete and insert
                if value_struct.payload.before.is_none()
                    && value_struct.payload.op == Some("u".to_string())
                {
                    value_struct.payload.before = value_struct.payload.after.clone();
                }

                match (value_struct.payload.after, value_struct.payload.before) {
                    (Some(new_payload), Some(old_payload)) => {
                        let new = convert_value_to_schema(new_payload, &schema, &fields_map)
                            .map_err(KafkaError::KafkaSchemaError)?;
                        let old = convert_value_to_schema(old_payload, &schema, &fields_map)
                            .map_err(KafkaError::KafkaSchemaError)?;

                        if ingestor
                            .handle_message(IngestionMessage::OperationEvent {
                                table_index: 0,
                                op: Operation::Update {
                                    old: Record {
                                        values: old,
                                        lifetime: None,
                                    },
                                    new: Record {
                                        values: new,
                                        lifetime: None,
                                    },
                                },
                                id: None,
                            })
                            .await
                            .is_err()
                        {
                            // If receiving side is closed, we should stop the stream
                            return Ok(());
                        }
                    }
                    (None, Some(old_payload)) => {
                        let old = convert_value_to_schema(old_payload, &schema, &fields_map)
                            .map_err(KafkaError::KafkaSchemaError)?;

                        if ingestor
                            .handle_message(IngestionMessage::OperationEvent {
                                table_index: 0,
                                op: Operation::Delete {
                                    old: Record {
                                        values: old,
                                        lifetime: None,
                                    },
                                },
                                id: None,
                            })
                            .await
                            .is_err()
                        {
                            // If receiving side is closed, we should stop the stream
                            return Ok(());
                        }
                    }
                    (Some(new_payload), None) => {
                        let new = convert_value_to_schema(new_payload, &schema, &fields_map)
                            .map_err(KafkaError::KafkaSchemaError)?;

                        if ingestor
                            .handle_message(IngestionMessage::OperationEvent {
                                table_index: 0,
                                op: Operation::Insert {
                                    new: Record {
                                        values: new,
                                        lifetime: None,
                                    },
                                },
                                id: None,
                            })
                            .await
                            .is_err()
                        {
                            // If receiving side is closed, we should stop the stream
                            return Ok(());
                        }
                    }
                    (None, None) => {}
                }
            }
        }
    }
}
