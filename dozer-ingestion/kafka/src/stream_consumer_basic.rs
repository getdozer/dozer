use std::collections::HashMap;

use dozer_ingestion_connector::{
    async_trait,
    dozer_types::{
        models::ingestion_types::IngestionMessage,
        node::OpIdentifier,
        serde::{Deserialize, Serialize},
        serde_json::{self, Value},
        types::{Field, Operation, Record},
    },
    Ingestor, TableInfo,
};
use rdkafka::{ClientConfig, Message};

use crate::schema_registry_basic::SchemaRegistryBasic;
use crate::stream_consumer::StreamConsumer;
use crate::{debezium::mapper::convert_value_to_schema, KafkaError};
use crate::{no_schema_registry_basic::NoSchemaRegistryBasic, KafkaStreamError};

use super::stream_consumer_helper::{is_network_failure, OffsetsMap, StreamConsumerHelper};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(crate = "dozer_ingestion_connector::dozer_types::serde")]
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
#[serde(crate = "dozer_ingestion_connector::dozer_types::serde")]
pub struct KafkaField {
    pub r#type: String,
    pub optional: bool,
    pub default: Option<FieldType>,
    pub field: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(crate = "dozer_ingestion_connector::dozer_types::serde")]
pub struct SchemaParameters {
    pub scale: Option<String>,
    #[serde(rename(deserialize = "connect.decimal.precision"))]
    pub precision: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(crate = "dozer_ingestion_connector::dozer_types::serde")]
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
#[serde(crate = "dozer_ingestion_connector::dozer_types::serde")]
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
        last_checkpoint: Option<OpIdentifier>,
        schema_registry_url: &Option<String>,
    ) -> Result<(), KafkaError> {
        assert!(last_checkpoint.is_none());
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
        loop {
            if let Some(result) = con.poll(None) {
                if matches!(result.as_ref(), Err(err) if is_network_failure(err)) {
                    con = StreamConsumerHelper::resume(&client_config, &topics, &offsets).await?;
                    continue;
                }
                let m = result
                    .map_err(|e| KafkaError::KafkaStreamError(KafkaStreamError::PollingError(e)))?;
                StreamConsumerHelper::update_offsets(&mut offsets, &m);
                match schemas.get(m.topic()) {
                    None => return Err(KafkaError::TopicNotDefined),
                    Some((table_index, (schema, fields_map))) => {
                        if let (Some(message), Some(key)) = (m.payload(), m.key()) {
                            let new = match schema_registry_url {
                                None => {
                                    let value = std::str::from_utf8(message)
                                        .map_err(KafkaError::BytesConvertError)?;
                                    let key = std::str::from_utf8(key)
                                        .map_err(KafkaError::BytesConvertError)?;

                                    vec![
                                        Field::String(key.to_string()),
                                        Field::String(value.to_string()),
                                    ]
                                }
                                Some(_) => {
                                    let value_struct: Value = serde_json::from_str(
                                        std::str::from_utf8(message)
                                            .map_err(KafkaError::BytesConvertError)?,
                                    )
                                    .map_err(KafkaError::JsonDecodeError)?;
                                    let _key_struct: Value = serde_json::from_str(
                                        std::str::from_utf8(key)
                                            .map_err(KafkaError::BytesConvertError)?,
                                    )
                                    .map_err(KafkaError::JsonDecodeError)?;

                                    convert_value_to_schema(
                                        value_struct,
                                        &schema.schema,
                                        fields_map,
                                    )
                                    .map_err(KafkaError::KafkaSchemaError)?
                                }
                            };

                            if ingestor
                                .handle_message(IngestionMessage::OperationEvent {
                                    table_index: *table_index,
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
                    }
                }
            }
        }
    }
}
