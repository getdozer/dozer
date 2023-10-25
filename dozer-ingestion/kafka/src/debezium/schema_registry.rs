use std::collections::HashMap;

use dozer_ingestion_connector::dozer_types::log::error;
use dozer_ingestion_connector::dozer_types::serde_json::{self, Value};
use dozer_ingestion_connector::dozer_types::types::{
    FieldDefinition, FieldType, Schema, SourceDefinition,
};
use dozer_ingestion_connector::{tokio, CdcType, SourceSchema};
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;

use crate::{KafkaError, KafkaSchemaError};

use super::schema::map_type;
use super::stream_consumer::DebeziumSchemaStruct;

pub struct SchemaRegistry {}

impl SchemaRegistry {
    pub fn map_typ(schema: &DebeziumSchemaStruct) -> Result<(FieldType, bool), KafkaSchemaError> {
        let nullable = schema.optional.map_or(false, |o| !o);
        match schema.r#type.clone() {
            Value::String(_) => map_type(&DebeziumSchemaStruct {
                r#type: schema.r#type.clone(),
                fields: None,
                optional: None,
                name: None,
                field: None,
                version: None,
                parameters: None,
            })
            .map(|s| (s, nullable)),
            Value::Array(types) => {
                let nullable = types.contains(&Value::from("null"));
                for typ in types {
                    if typ.as_str().unwrap() != "null" {
                        return Self::map_typ(&DebeziumSchemaStruct {
                            r#type: typ,
                            fields: None,
                            optional: Some(nullable),
                            name: None,
                            field: None,
                            version: None,
                            parameters: None,
                        });
                    }
                }

                Err(KafkaSchemaError::TypeNotSupported("Array".to_string()))
            }
            Value::Object(obj) => SchemaRegistry::map_typ(&DebeziumSchemaStruct {
                r#type: obj.get("type").unwrap().clone(),
                fields: None,
                optional: None,
                name: None,
                field: None,
                version: None,
                parameters: None,
            }),
            _ => Err(KafkaSchemaError::TypeNotSupported(
                "Unexpected value".to_string(),
            )),
        }
    }

    pub async fn fetch_struct(
        sr_settings: &SrSettings,
        table_name: &str,
        is_key: bool,
    ) -> Result<DebeziumSchemaStruct, KafkaError> {
        let schema_result = loop {
            match schema_registry_converter::async_impl::schema_registry::get_schema_by_subject(
                sr_settings,
                &SubjectNameStrategy::TopicNameStrategy(table_name.to_string(), is_key),
            )
            .await
            {
                Ok(schema_result) => break schema_result,
                Err(err) if err.retriable => {
                    const RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
                    error!("schema registry fetch error {err}. retrying in {RETRY_INTERVAL:?}...");
                    tokio::time::sleep(RETRY_INTERVAL).await;
                    continue;
                }
                Err(err) => return Err(KafkaError::SchemaRegistryFetchError(err)),
            }
        };

        serde_json::from_str::<DebeziumSchemaStruct>(&schema_result.schema)
            .map_err(KafkaError::JsonDecodeError)
    }

    pub async fn get_schema(
        table_names: Option<&[String]>,
        schema_registry_url: String,
    ) -> Result<Vec<SourceSchema>, KafkaError> {
        let sr_settings = SrSettings::new(schema_registry_url);
        match table_names {
            None => Ok(vec![]),
            Some(tables) => match tables.get(0) {
                None => Ok(vec![]),
                Some(table) => {
                    let key_result =
                        SchemaRegistry::fetch_struct(&sr_settings, table, true).await?;
                    let schema_result =
                        SchemaRegistry::fetch_struct(&sr_settings, table, false).await?;

                    let pk_fields = key_result.fields.map_or(vec![], |fields| {
                        fields
                            .iter()
                            .map(|f| f.name.clone().map_or("".to_string(), |name| name))
                            .collect()
                    });

                    let fields = schema_result.fields.map_or(vec![], |f| f);
                    let mut pk_keys_indexes = vec![];
                    let mut fields_schema_map: HashMap<String, &DebeziumSchemaStruct> =
                        HashMap::new();

                    let defined_fields: Result<Vec<FieldDefinition>, KafkaError> = fields
                        .iter()
                        .enumerate()
                        .map(|(idx, f)| {
                            let (typ, nullable) =
                                Self::map_typ(f).map_err(KafkaError::KafkaSchemaError)?;
                            let name = f.name.clone().unwrap();
                            if pk_fields.contains(&name) {
                                pk_keys_indexes.push(idx);
                            }
                            fields_schema_map.insert(name.clone(), f);
                            Ok(FieldDefinition {
                                name,
                                typ,
                                nullable,
                                source: SourceDefinition::Dynamic,
                            })
                        })
                        .collect();

                    let schema = Schema {
                        fields: defined_fields?,
                        primary_index: pk_keys_indexes,
                    };

                    Ok(vec![SourceSchema::new(schema, CdcType::FullChanges)])
                }
            },
        }
    }
}
