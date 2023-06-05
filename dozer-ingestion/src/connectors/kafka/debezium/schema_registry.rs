#![allow(clippy::type_complexity)]

use crate::connectors::kafka::debezium::schema::map_type;
use crate::connectors::kafka::debezium::stream_consumer::DebeziumSchemaStruct;
use crate::connectors::{CdcType, SourceSchema};
use crate::errors::DebeziumError::{JsonDecodeError, SchemaRegistryFetchError};
use crate::errors::DebeziumSchemaError::TypeNotSupported;
use crate::errors::{ConnectorError, DebeziumError, DebeziumSchemaError};
use dozer_types::serde_json;
use dozer_types::serde_json::Value;
use dozer_types::types::{FieldDefinition, FieldType, Schema, SchemaIdentifier, SourceDefinition};
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
use std::collections::HashMap;

pub struct SchemaRegistry {}

impl SchemaRegistry {
    pub fn map_typ(
        schema: &DebeziumSchemaStruct,
    ) -> Result<(FieldType, bool), DebeziumSchemaError> {
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

                Err(TypeNotSupported("Array".to_string()))
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
            _ => Err(TypeNotSupported("Unexpected value".to_string())),
        }
    }

    pub async fn fetch_struct(
        sr_settings: &SrSettings,
        table_name: &str,
        is_key: bool,
    ) -> Result<DebeziumSchemaStruct, DebeziumError> {
        let schema_result =
            schema_registry_converter::async_impl::schema_registry::get_schema_by_subject(
                sr_settings,
                &SubjectNameStrategy::TopicNameStrategy(table_name.to_string(), is_key),
            )
            .await
            .map_err(SchemaRegistryFetchError)?;

        serde_json::from_str::<DebeziumSchemaStruct>(&schema_result.schema).map_err(JsonDecodeError)
    }

    pub async fn get_schema(
        table_names: Option<&[String]>,
        schema_registry_url: String,
    ) -> Result<Vec<SourceSchema>, ConnectorError> {
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

                    let defined_fields: Result<Vec<FieldDefinition>, ConnectorError> = fields
                        .iter()
                        .enumerate()
                        .map(|(idx, f)| {
                            let (typ, nullable) = Self::map_typ(f).map_err(|e| {
                                ConnectorError::DebeziumError(DebeziumError::DebeziumSchemaError(e))
                            })?;
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
                        identifier: Some(SchemaIdentifier { id: 1, version: 1 }),
                        fields: defined_fields?,
                        primary_index: pk_keys_indexes,
                    };

                    Ok(vec![SourceSchema::new(schema, CdcType::FullChanges)])
                }
            },
        }
    }
}
