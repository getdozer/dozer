use crate::connectors::kafka::debezium::schema::SchemaFetcher;
use crate::connectors::kafka::debezium::stream_consumer::DebeziumSchemaStruct;
use crate::connectors::TableInfo;
use crate::errors::DebeziumError::JsonDecodeError;
use crate::errors::DebeziumSchemaError::TypeNotSupported;
use crate::errors::{ConnectorError, DebeziumSchemaError};
use dozer_types::ingestion_types::KafkaConfig;
use dozer_types::serde_json;
use dozer_types::serde_json::Value;
use dozer_types::types::{FieldDefinition, FieldType, Schema, SchemaIdentifier};
use schema_registry_converter::blocking::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
use std::collections::HashMap;

pub struct SchemaRegistry {}

pub fn map_typ(schema: &DebeziumSchemaStruct) -> Result<(FieldType, bool), DebeziumSchemaError> {
    let nullable = schema.optional.map_or(false, |o| !o);
    match schema.r#type.clone() {
        Value::String(typ) => match typ.as_str() {
            "int" | "int8" | "int16" | "int32" | "int64" => Ok((FieldType::Int, nullable)),
            "string" => Ok((FieldType::String, nullable)),
            "bytes" => Ok((FieldType::Binary, nullable)),
            "float32" | "float64" | "double" => Ok((FieldType::Float, nullable)),
            "boolean" => Ok((FieldType::Boolean, nullable)),
            _ => Err(TypeNotSupported(typ)),
        },
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

            Err(DebeziumSchemaError::TypeNotSupported("Array".to_string()))
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

impl SchemaFetcher for SchemaRegistry {
    fn get_schema(
        table_names: Option<Vec<TableInfo>>,
        config: KafkaConfig,
    ) -> Result<Vec<(String, dozer_types::types::Schema)>, ConnectorError> {
        let sr_settings = SrSettings::new(config.schema_registry_url.unwrap());
        table_names.map_or(Ok(vec![]), |tables| {
            tables.get(0).map_or(Ok(vec![]), |table| {
                let schema_result =
                    schema_registry_converter::blocking::schema_registry::get_schema_by_subject(
                        &sr_settings,
                        &SubjectNameStrategy::TopicNameStrategy(table.clone().name, false),
                    )
                    .unwrap();
                eprintln!("a: {:?}", schema_result.schema);

                let result: DebeziumSchemaStruct =
                    serde_json::from_str(&schema_result.schema).map_err(JsonDecodeError)?;

                let mut schema_data: Option<
                    Result<Vec<(String, dozer_types::types::Schema)>, ConnectorError>,
                > = None;
                result.fields.iter().for_each(|field| {
                    field.iter().for_each(|f| {
                        if f.name.clone().unwrap() == "before" {
                            for typ in f.r#type.as_array().unwrap() {
                                if let Value::Object(obj) = typ {
                                    let fields_value = obj.get("fields").unwrap();
                                    let fields_value_struct: Vec<DebeziumSchemaStruct> =
                                        serde_json::from_value(fields_value.clone()).unwrap();
                                    // let mut pk_keys_indexes = vec![];
                                    let mut fields_schema_map: HashMap<
                                        String,
                                        &DebeziumSchemaStruct,
                                    > = HashMap::new();

                                    let defined_fields: Result<
                                        Vec<FieldDefinition>,
                                        DebeziumSchemaError,
                                    > = fields_value_struct
                                        .iter()
                                        .enumerate()
                                        .map(|(_idx, f)| {
                                            let (typ, nullable) = map_typ(f)?;
                                            let name = f.name.clone().unwrap();
                                            // if pk_fields.contains(&name) {
                                            //     pk_keys_indexes.push(idx);
                                            // }
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
                                        fields: defined_fields.unwrap(),
                                        values: vec![],
                                        primary_index: vec![],
                                    };

                                    schema_data = Some(Ok(vec![(table.name.clone(), schema)]));
                                }
                            }
                        }
                    });
                });

                if let Some(v) = schema_data {
                    v
                } else {
                    Ok(vec![])
                }
            })
        })
    }
}
