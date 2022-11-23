use std::sync::Arc;

use crate::connectors::Connector;
use crate::ingestion::Ingestor;
use crate::{connectors::TableInfo, errors::ConnectorError};
use dozer_types::ingestion_types::{IngestionMessage, KafkaConfig};

use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Operation, OperationEvent, Record, SchemaIdentifier};
use tokio::runtime::Runtime;

use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

use crate::connectors::kafka::helper::mapper::convert_value_to_schema;
use crate::connectors::kafka::helper::schema::map_schema;
use crate::errors::DebeziumError::{BytesConvertError, DebeziumConnectionError, JsonDecodeError};
use crate::errors::{DebeziumError, DebeziumStreamError};
use dozer_types::serde::{Deserialize, Serialize};
use dozer_types::serde_json;
use dozer_types::serde_json::Value;

pub struct KafkaConnector {
    pub id: u64,
    config: KafkaConfig,
    ingestor: Option<Arc<RwLock<Ingestor>>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
#[serde(untagged)]
pub enum KafkaFieldType {
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub struct KafkaField {
    pub r#type: String,
    pub optional: bool,
    pub default: Option<KafkaFieldType>,
    pub field: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub struct KafkaSchemaParameters {
    pub scale: Option<String>,
    #[serde(rename(deserialize = "connect.decimal.precision"))]
    pub precision: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub struct KafkaSchemaStruct {
    pub r#type: String,
    pub fields: Option<Vec<KafkaSchemaStruct>>,
    pub optional: bool,
    pub name: Option<String>,
    pub field: Option<String>,
    pub version: Option<i64>,
    pub parameters: Option<KafkaSchemaParameters>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub struct KafkaPayload {
    pub before: Option<Value>,
    pub after: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub struct KafkaMessageStruct {
    pub schema: KafkaSchemaStruct,
    pub payload: KafkaPayload,
}

impl KafkaConnector {
    pub fn new(id: u64, config: KafkaConfig) -> Self {
        Self {
            id,
            config,
            ingestor: None,
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
        _: Option<Vec<TableInfo>>,
    ) -> Result<(), ConnectorError> {
        self.ingestor = Some(ingestor);
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

    fn validate(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

async fn run(
    broker: String,
    topic: String,
    ingestor: Arc<RwLock<Ingestor>>,
    connector_id: u64,
) -> Result<(), ConnectorError> {
    let mut con = Consumer::from_hosts(vec![broker])
        .with_topic(topic)
        .with_fallback_offset(FetchOffset::Earliest)
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()
        .map_err(DebeziumConnectionError)?;

    loop {
        let mss = con.poll().map_err(|e| {
            DebeziumError::DebeziumStreamError(DebeziumStreamError::PollingError(e))
        })?;
        if !mss.is_empty() {
            for ms in mss.iter() {
                for m in ms.messages() {
                    let value_struct: KafkaMessageStruct = serde_json::from_str(
                        std::str::from_utf8(m.value).map_err(BytesConvertError)?,
                    )
                    .map_err(JsonDecodeError)?;
                    let key_struct: KafkaMessageStruct = serde_json::from_str(
                        std::str::from_utf8(m.key).map_err(BytesConvertError)?,
                    )
                    .map_err(JsonDecodeError)?;

                    let (schema, fields_map) = map_schema(&value_struct.schema, &key_struct.schema)
                        .map_err(|e| {
                            ConnectorError::DebeziumError(DebeziumError::DebeziumSchemaError(e))
                        })?;

                    ingestor
                        .write()
                        .handle_message((
                            connector_id,
                            IngestionMessage::Schema("customerss".to_string(), schema.clone()),
                        ))
                        .map_err(ConnectorError::IngestorError)?;

                    match (value_struct.payload.after, value_struct.payload.before) {
                        (Some(new_payload), Some(old_payload)) => {
                            let new = convert_value_to_schema(
                                new_payload,
                                schema.clone(),
                                fields_map.clone(),
                            )
                            .map_err(|e| {
                                ConnectorError::DebeziumError(DebeziumError::DebeziumSchemaError(e))
                            })?;
                            let old =
                                convert_value_to_schema(old_payload, schema.clone(), fields_map)
                                    .map_err(|e| {
                                        ConnectorError::DebeziumError(
                                            DebeziumError::DebeziumSchemaError(e),
                                        )
                                    })?;

                            ingestor
                                .write()
                                .handle_message((
                                    connector_id,
                                    IngestionMessage::OperationEvent(OperationEvent {
                                        seq_no: 0,
                                        operation: Operation::Update {
                                            old: Record {
                                                schema_id: Some(SchemaIdentifier {
                                                    id: 1,
                                                    version: 1,
                                                }),
                                                values: old,
                                            },
                                            new: Record {
                                                schema_id: Some(SchemaIdentifier {
                                                    id: 1,
                                                    version: 1,
                                                }),
                                                values: new,
                                            },
                                        },
                                    }),
                                ))
                                .map_err(ConnectorError::IngestorError)?;
                        }
                        (None, Some(old_payload)) => {
                            let old = convert_value_to_schema(old_payload, schema, fields_map)
                                .map_err(|e| {
                                    ConnectorError::DebeziumError(
                                        DebeziumError::DebeziumSchemaError(e),
                                    )
                                })?;

                            ingestor
                                .write()
                                .handle_message((
                                    connector_id,
                                    IngestionMessage::OperationEvent(OperationEvent {
                                        seq_no: 0,
                                        operation: Operation::Delete {
                                            old: Record {
                                                schema_id: Some(SchemaIdentifier {
                                                    id: 1,
                                                    version: 1,
                                                }),
                                                values: old,
                                            },
                                        },
                                    }),
                                ))
                                .map_err(ConnectorError::IngestorError)?;
                        }
                        (Some(new_payload), None) => {
                            let new = convert_value_to_schema(
                                new_payload,
                                schema.clone(),
                                fields_map.clone(),
                            )
                            .map_err(|e| {
                                ConnectorError::DebeziumError(DebeziumError::DebeziumSchemaError(e))
                            })?;
                            //
                            ingestor
                                .write()
                                .handle_message((
                                    connector_id,
                                    IngestionMessage::OperationEvent(OperationEvent {
                                        seq_no: 0,
                                        operation: Operation::Insert {
                                            new: Record {
                                                schema_id: Some(SchemaIdentifier {
                                                    id: 1,
                                                    version: 1,
                                                }),
                                                values: new,
                                            },
                                        },
                                    }),
                                ))
                                .map_err(ConnectorError::IngestorError)?;
                        }
                        (None, None) => {}
                    }
                }

                con.consume_messageset(ms).map_err(|e| {
                    DebeziumError::DebeziumStreamError(DebeziumStreamError::MessageConsumeError(e))
                })?;
            }
            con.commit_consumed().map_err(|e| {
                DebeziumError::DebeziumStreamError(DebeziumStreamError::ConsumeCommitError(e))
            })?;
        }
    }
}
