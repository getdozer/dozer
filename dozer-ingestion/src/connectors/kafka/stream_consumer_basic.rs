use crate::connectors::kafka::debezium::mapper::convert_value_to_schema;
use std::collections::HashMap;

use crate::connectors::kafka::stream_consumer::StreamConsumer;
use crate::errors::KafkaError::{
    BytesConvertError, JsonDecodeError, KafkaStreamError, TopicNotDefined,
};
use crate::errors::{ConnectorError, KafkaError};
use crate::ingestion::Ingestor;
use dozer_types::ingestion_types::IngestionMessage;

use dozer_types::serde::{Deserialize, Serialize};
use dozer_types::serde_json;
use dozer_types::serde_json::Value;
use dozer_types::types::{Field, Operation, Record};

use crate::connectors::kafka::no_schema_registry_basic::NoSchemaRegistryBasic;
use crate::connectors::kafka::schema_registry_basic::SchemaRegistryBasic;
use tonic::async_trait;

use crate::connectors::TableInfo;
use crate::errors::KafkaStreamError::PollingError;
use rdkafka::{ClientConfig, Message};

use super::stream_consumer_helper::{is_network_failure, OffsetsMap, StreamConsumerHelper};

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

#[derive(Default)]
pub struct StreamConsumerBasic {}

#[async_trait]
impl StreamConsumer for StreamConsumerBasic {
    async fn run(
        &self,
        client_config: ClientConfig,
        ingestor: &Ingestor,
        tables: Vec<TableInfo>,
        schema_registry_url: &Option<String>,
    ) -> Result<(), ConnectorError> {
        let topics: Vec<String> = tables.iter().map(|t| t.name.clone()).collect();

        let mut schemas = HashMap::new();
        for (table_index, table) in tables.into_iter().enumerate() {
            let schema = if let Some(url) = schema_registry_url {
                SchemaRegistryBasic::get_single_schema(&table.name, url).await?
            } else {
                (NoSchemaRegistryBasic::get_single_schema(), HashMap::new())
            };

            schemas.insert(table.name.clone(), (table_index, schema));
        }

        let topics: Vec<&str> = topics.iter().map(|t| t.as_str()).collect();
        let mut con = StreamConsumerHelper::start(&client_config, &topics).await?;

        let mut offsets = OffsetsMap::new();
        let mut counter = 0;
        loop {
            if let Some(result) = con.poll(None) {
                if matches!(result.as_ref(), Err(err) if is_network_failure(err)) {
                    con = StreamConsumerHelper::resume(&client_config, &topics, &offsets).await?;
                    continue;
                }
                let m = result.map_err(|e| KafkaStreamError(PollingError(e)))?;
                StreamConsumerHelper::update_offsets(&mut offsets, &m);
                match schemas.get(m.topic()) {
                    None => return Err(ConnectorError::KafkaError(TopicNotDefined)),
                    Some((table_index, (schema, fields_map))) => {
                        if let (Some(message), Some(key)) = (m.payload(), m.key()) {
                            let new = match schema_registry_url {
                                None => {
                                    let value =
                                        std::str::from_utf8(message).map_err(BytesConvertError)?;
                                    let key =
                                        std::str::from_utf8(key).map_err(BytesConvertError)?;

                                    vec![
                                        Field::String(key.to_string()),
                                        Field::String(value.to_string()),
                                    ]
                                }
                                Some(_) => {
                                    let value_struct: Value = serde_json::from_str(
                                        std::str::from_utf8(message).map_err(BytesConvertError)?,
                                    )
                                    .map_err(JsonDecodeError)?;
                                    let _key_struct: Value = serde_json::from_str(
                                        std::str::from_utf8(key).map_err(BytesConvertError)?,
                                    )
                                    .map_err(JsonDecodeError)?;

                                    convert_value_to_schema(
                                        value_struct,
                                        &schema.schema,
                                        fields_map,
                                    )
                                    .map_err(|e| {
                                        ConnectorError::KafkaError(KafkaError::KafkaSchemaError(e))
                                    })?
                                }
                            };

                            ingestor
                                .handle_message(IngestionMessage::new_op(
                                    0,
                                    counter,
                                    *table_index,
                                    Operation::Insert {
                                        new: Record {
                                            values: new,
                                            lifetime: None,
                                        },
                                    },
                                ))
                                .map_err(ConnectorError::IngestorError)?;

                            counter += 1;
                        }
                    }
                }
            }
        }
    }
}
