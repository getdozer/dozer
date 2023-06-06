use crate::connectors::kafka::debezium::mapper::convert_value_to_schema;

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

use crate::connectors::kafka::schema_registry_basic::SchemaRegistryBasic;
use tonic::async_trait;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(crate = "dozer_types::serde")]
#[serde(untagged)]
pub enum FieldType {
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
#[serde(crate = "dozer_types::serde")]
pub struct KafkaField {
    pub r#type: String,
    pub optional: bool,
    pub default: Option<FieldType>,
    pub field: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(crate = "dozer_types::serde")]
pub struct SchemaParameters {
    pub scale: Option<String>,
    #[serde(rename(deserialize = "connect.decimal.precision"))]
    pub precision: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(crate = "dozer_types::serde")]
pub struct SchemaStruct {
    pub r#type: Value,
    pub fields: Option<Vec<SchemaStruct>>,
    pub optional: Option<bool>,
    pub name: Option<String>,
    pub field: Option<String>,
    pub version: Option<i64>,
    pub parameters: Option<SchemaParameters>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(crate = "dozer_types::serde")]
pub struct Payload {
    pub before: Option<Value>,
    pub after: Option<Value>,
    pub op: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(crate = "dozer_types::serde")]
pub struct Message {
    pub schema: SchemaStruct,
    pub payload: Payload,
}

#[derive(Default)]
pub struct StreamConsumerBasic {}

impl StreamConsumerBasic {}

#[async_trait]
impl StreamConsumer for StreamConsumerBasic {
    async fn run(
        &self,
        mut con: Consumer,
        ingestor: &Ingestor,
        topic: &str,
        schema_registry_url: &Option<String>,
    ) -> Result<(), ConnectorError> {
        let (schema, fields_map) =
            SchemaRegistryBasic::get_single_schema(topic, schema_registry_url.as_ref().unwrap())
                .await?;
        let mut counter = 0;
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

                        let value_struct: Value = serde_json::from_str(
                            std::str::from_utf8(m.value).map_err(BytesConvertError)?,
                        )
                        .map_err(JsonDecodeError)?;
                        let _key_struct: Value = serde_json::from_str(
                            std::str::from_utf8(m.key).map_err(BytesConvertError)?,
                        )
                        .map_err(JsonDecodeError)?;

                        let new =
                            convert_value_to_schema(value_struct, &schema.schema, &fields_map)
                                .map_err(|e| {
                                    ConnectorError::DebeziumError(
                                        DebeziumError::DebeziumSchemaError(e),
                                    )
                                })?;

                        ingestor
                            .handle_message(IngestionMessage::new_op(
                                0,
                                counter,
                                Operation::Insert {
                                    new: Record {
                                        schema_id: Some(SchemaIdentifier { id: 1, version: 1 }),
                                        values: new,
                                        lifetime: None,
                                    },
                                },
                            ))
                            .map_err(ConnectorError::IngestorError)?;

                        counter += 1;
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
