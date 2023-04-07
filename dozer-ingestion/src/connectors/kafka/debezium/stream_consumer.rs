use crate::connectors::kafka::debezium::mapper::convert_value_to_schema;
use crate::connectors::kafka::debezium::schema::map_schema;
use crate::connectors::kafka::stream_consumer::StreamConsumer;
use crate::errors::DebeziumError::{BytesConvertError, JsonDecodeError};
use crate::errors::{ConnectorError, DebeziumError, DebeziumStreamError};
use crate::ingestion::Ingestor;
use dozer_types::ingestion_types::IngestionMessage;

use dozer_types::serde::{Deserialize, Serialize};
use dozer_types::serde_json;
use dozer_types::serde_json::Value;
use dozer_types::types::{Operation, Record, SchemaIdentifier};
use kafka::consumer::Consumer;

#[derive(Debug, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub struct DebeziumField {
    pub r#type: String,
    pub optional: bool,
    pub default: Option<DebeziumFieldType>,
    pub field: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(crate = "dozer_types::serde")]
pub struct DebeziumSchemaParameters {
    pub scale: Option<String>,
    #[serde(rename(deserialize = "connect.decimal.precision"))]
    pub precision: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(crate = "dozer_types::serde")]
pub struct DebeziumSchemaStruct {
    pub r#type: Value,
    pub fields: Option<Vec<DebeziumSchemaStruct>>,
    pub optional: Option<bool>,
    pub name: Option<String>,
    pub field: Option<String>,
    pub version: Option<i64>,
    pub parameters: Option<DebeziumSchemaParameters>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub struct DebeziumPayload {
    pub before: Option<Value>,
    pub after: Option<Value>,
    pub op: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub struct DebeziumMessage {
    pub schema: DebeziumSchemaStruct,
    pub payload: DebeziumPayload,
}

#[derive(Default)]
pub struct DebeziumStreamConsumer {}

impl DebeziumStreamConsumer {}

impl StreamConsumer for DebeziumStreamConsumer {
    fn run(&self, mut con: Consumer, ingestor: &Ingestor) -> Result<(), ConnectorError> {
        loop {
            let mss = con.poll().map_err(|e| {
                DebeziumError::DebeziumStreamError(DebeziumStreamError::PollingError(e))
            })?;
            if !mss.is_empty() {
                for ms in mss.iter() {
                    for m in ms.messages() {
                        if m.value.is_empty() {
                            continue;
                        }

                        let mut value_struct: DebeziumMessage = serde_json::from_str(
                            std::str::from_utf8(m.value).map_err(BytesConvertError)?,
                        )
                        .map_err(JsonDecodeError)?;
                        let key_struct: DebeziumMessage = serde_json::from_str(
                            std::str::from_utf8(m.key).map_err(BytesConvertError)?,
                        )
                        .map_err(JsonDecodeError)?;

                        let (schema, fields_map) =
                            map_schema(&value_struct.schema, &key_struct.schema).map_err(|e| {
                                ConnectorError::DebeziumError(DebeziumError::DebeziumSchemaError(e))
                            })?;

                        // When update happens before is null.
                        // If PK value changes, then debezium creates two events - delete and insert
                        if value_struct.payload.before.is_none()
                            && value_struct.payload.op == Some("u".to_string())
                        {
                            value_struct.payload.before = value_struct.payload.after.clone();
                        }

                        match (value_struct.payload.after, value_struct.payload.before) {
                            (Some(new_payload), Some(old_payload)) => {
                                let new = convert_value_to_schema(
                                    new_payload,
                                    schema.clone(),
                                    fields_map.clone(),
                                )
                                .map_err(|e| {
                                    ConnectorError::DebeziumError(
                                        DebeziumError::DebeziumSchemaError(e),
                                    )
                                })?;
                                let old = convert_value_to_schema(
                                    old_payload,
                                    schema.clone(),
                                    fields_map,
                                )
                                .map_err(|e| {
                                    ConnectorError::DebeziumError(
                                        DebeziumError::DebeziumSchemaError(e),
                                    )
                                })?;

                                ingestor
                                    .handle_message(IngestionMessage::new_op(
                                        0,
                                        0,
                                        Operation::Update {
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
                                    .handle_message(IngestionMessage::new_op(
                                        0,
                                        0,
                                        Operation::Delete {
                                            old: Record {
                                                schema_id: Some(SchemaIdentifier {
                                                    id: 1,
                                                    version: 1,
                                                }),
                                                values: old,
                                            },
                                        },
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
                                    ConnectorError::DebeziumError(
                                        DebeziumError::DebeziumSchemaError(e),
                                    )
                                })?;

                                ingestor
                                    .handle_message(IngestionMessage::new_op(
                                        0,
                                        0,
                                        Operation::Insert {
                                            new: Record {
                                                schema_id: Some(SchemaIdentifier {
                                                    id: 1,
                                                    version: 1,
                                                }),
                                                values: new,
                                            },
                                        },
                                    ))
                                    .map_err(ConnectorError::IngestorError)?;
                            }
                            (None, None) => {}
                        }
                    }

                    con.consume_messageset(ms).map_err(|e| {
                        DebeziumError::DebeziumStreamError(
                            DebeziumStreamError::MessageConsumeError(e),
                        )
                    })?;
                }
                con.commit_consumed().map_err(|e| {
                    DebeziumError::DebeziumStreamError(DebeziumStreamError::ConsumeCommitError(e))
                })?;
            }
        }
    }
}
