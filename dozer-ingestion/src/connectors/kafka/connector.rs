use std::sync::Arc;

use crate::connectors::Connector;
use crate::ingestion::Ingestor;
use crate::{connectors::TableInfo, errors::ConnectorError};
use dozer_types::ingestion_types::KafkaConfig;
use dozer_types::log::{info};
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{FieldDefinition, FieldType, Schema, SchemaIdentifier};
use tokio::runtime::Runtime;

use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

use crate::connectors::kafka::helper::schema::map_schema;
use dozer_types::serde::{Deserialize, Serialize};
use dozer_types::serde_json;


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
pub struct KafkaSchemaStruct {
    pub r#type: String,
    pub fields: Option<Vec<KafkaSchemaStruct>>,
    pub optional: bool,
    pub name: Option<String>,
    pub field: Option<String>,
    pub version: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub struct KafkaMessageStruct {
    pub schema: KafkaSchemaStruct,
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
        Ok(vec![(
            "dbserver1.inventory.customers".to_string(),
            Schema {
                identifier: Some(SchemaIdentifier {
                    id: 333,
                    version: 1,
                }),
                fields: vec![
                    FieldDefinition {
                        name: "id".to_string(),
                        typ: FieldType::Int,
                        nullable: false,
                    },
                    FieldDefinition {
                        name: "first_name".to_string(),
                        typ: FieldType::String,
                        nullable: false,
                    },
                    FieldDefinition {
                        name: "last_name".to_string(),
                        typ: FieldType::String,
                        nullable: false,
                    },
                ],
                values: vec![],
                // Log Index
                primary_index: vec![0],
                secondary_indexes: vec![],
            },
        )])
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

#[allow(unreachable_code)]
async fn run(
    broker: String,
    topic: String,
    _ingestor: Arc<RwLock<Ingestor>>,
    _connector_id: u64,
) -> Result<(), ConnectorError> {
    let mut con = Consumer::from_hosts(vec![broker])
        .with_topic(topic)
        .with_fallback_offset(FetchOffset::Earliest)
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()
        .unwrap();

    loop {
        let mss = con.poll().unwrap();
        if !mss.is_empty() {
            for ms in mss.iter() {
                for m in ms.messages() {
                    let value_struct: KafkaMessageStruct =
                        serde_json::from_str(std::str::from_utf8(m.value).unwrap()).unwrap();
                    let key_struct: KafkaMessageStruct =
                        serde_json::from_str(std::str::from_utf8(m.key).unwrap()).unwrap();

                    let schema = map_schema(&value_struct.schema, &key_struct.schema);
                    info!("Schema: {:?}", schema);
                }
                let _ = con.consume_messageset(ms);
            }
            con.commit_consumed().unwrap();
        }
    }
}
