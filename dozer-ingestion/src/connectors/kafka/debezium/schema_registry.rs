#![allow(clippy::type_complexity)]

use crate::connectors::kafka::debezium::schema::map_type;
use crate::connectors::kafka::debezium::stream_consumer::DebeziumSchemaStruct;
use crate::connectors::TableInfo;
use crate::errors::DebeziumError::{JsonDecodeError, SchemaRegistryFetchError};
use crate::errors::DebeziumSchemaError::TypeNotSupported;
use crate::errors::{ConnectorError, DebeziumError, DebeziumSchemaError};
use dozer_types::ingestion_types::KafkaConfig;
use dozer_types::serde_json;
use dozer_types::serde_json::Value;
use dozer_types::types::{
    FieldDefinition, FieldType, ReplicationChangesTrackingType, Schema, SchemaIdentifier,
    SchemaWithChangesType,
};
use schema_registry_converter::blocking::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
use std::collections::HashMap;

pub struct SchemaRegistry {}

pub fn map_typ(schema: &DebeziumSchemaStruct) -> Result<(FieldType, bool), DebeziumSchemaError> {
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
                    return map_typ(&DebeziumSchemaStruct {
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
        Value::Object(obj) => map_typ(&DebeziumSchemaStruct {
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

impl SchemaRegistry {
    pub fn fetch_struct(
        sr_settings: &SrSettings,
        table_name: &str,
        is_key: bool,
    ) -> Result<DebeziumSchemaStruct, DebeziumError> {
        let schema_result =
            schema_registry_converter::blocking::schema_registry::get_schema_by_subject(
                sr_settings,
                &SubjectNameStrategy::TopicNameStrategy(table_name.to_string(), is_key),
            )
            .map_err(SchemaRegistryFetchError)?;

        serde_json::from_str::<DebeziumSchemaStruct>(&schema_result.schema).map_err(JsonDecodeError)
    }

    pub fn get_schema(
        table_names: Option<Vec<TableInfo>>,
        config: KafkaConfig,
    ) -> Result<Vec<SchemaWithChangesType>, ConnectorError> {
        let sr_settings = SrSettings::new(config.schema_registry_url.unwrap());
        table_names.map_or(Ok(vec![]), |tables| {
            tables.get(0).map_or(Ok(vec![]), |table| {
                let key_result = SchemaRegistry::fetch_struct(&sr_settings, &table.name, true)?;
                let schema_result = SchemaRegistry::fetch_struct(&sr_settings, &table.name, false)?;

                let pk_fields = key_result.fields.map_or(vec![], |fields| {
                    fields
                        .iter()
                        .map(|f| f.name.clone().map_or("".to_string(), |name| name))
                        .collect()
                });

                let mut schema_data: Option<Result<Vec<SchemaWithChangesType>, ConnectorError>> =
                    None;
                let fields = schema_result.fields.map_or(vec![], |f| f);
                for f in fields {
                    if f.name.clone().unwrap() == "before" {
                        for typ in f.r#type.as_array().unwrap() {
                            if let Value::Object(obj) = typ {
                                let fields_value = obj.get("fields").unwrap();
                                let fields_value_struct: Vec<DebeziumSchemaStruct> =
                                    serde_json::from_value(fields_value.clone()).unwrap();
                                let mut pk_keys_indexes = vec![];
                                let mut fields_schema_map: HashMap<String, &DebeziumSchemaStruct> =
                                    HashMap::new();

                                let defined_fields: Result<Vec<FieldDefinition>, ConnectorError> =
                                    fields_value_struct
                                        .iter()
                                        .enumerate()
                                        .map(|(idx, f)| {
                                            let (typ, nullable) = map_typ(f).map_err(|e| {
                                                ConnectorError::DebeziumError(
                                                    DebeziumError::DebeziumSchemaError(e),
                                                )
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
                                            })
                                        })
                                        .collect();

                                let schema = Schema {
                                    identifier: Some(SchemaIdentifier { id: 1, version: 1 }),
                                    fields: defined_fields?,
                                    primary_index: pk_keys_indexes,
                                };

                                schema_data = Some(Ok(vec![(
                                    table.name.clone(),
                                    schema,
                                    ReplicationChangesTrackingType::FullChanges,
                                )]));
                            }
                        }
                    }
                }

                if let Some(v) = schema_data {
                    v
                } else {
                    Ok(vec![])
                }
            })
        })
    }
}
