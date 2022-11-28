use std::collections::HashMap;

use crate::connectors::kafka::debezium::stream_consumer::DebeziumSchemaStruct;
use crate::errors::DebeziumSchemaError;
use crate::errors::DebeziumSchemaError::{SchemaDefinitionNotFound, TypeNotSupported};
use dozer_types::types::{FieldDefinition, FieldType, Schema, SchemaIdentifier};

// Reference: https://debezium.io/documentation/reference/0.9/connectors/postgresql.html
fn map_type(schema: &DebeziumSchemaStruct) -> Result<FieldType, DebeziumSchemaError> {
    match schema.name.clone() {
        None => match schema.r#type.as_str() {
            "int8" | "int16" | "int32" | "int64" => Ok(FieldType::Int),
            "string" => Ok(FieldType::String),
            "bytes" => Ok(FieldType::Binary),
            "float32" | "float64" | "double" => Ok(FieldType::Float),
            "boolean" => Ok(FieldType::Boolean),
            type_name => Err(TypeNotSupported(type_name.to_string())),
        },
        Some(name) => match name.as_str() {
            "io.debezium.time.MicroTime"
            | "io.debezium.time.Timestamp"
            | "io.debezium.time.MicroTimestamp"
            | "org.apache.kafka.connect.data.Time"
            | "org.apache.kafka.connect.data.Timestamp" => Ok(FieldType::Timestamp),
            "io.debezium.time.Date" | "org.apache.kafka.connect.data.Date" => Ok(FieldType::Date),
            "org.apache.kafka.connect.data.Decimal" | "io.debezium.data.VariableScaleDecimal" => {
                Ok(FieldType::Decimal)
            }
            "io.debezium.data.Json" => Ok(FieldType::Bson),
            _ => Err(TypeNotSupported(name)),
        },
    }
}

pub fn map_schema<'a>(
    schema: &'a DebeziumSchemaStruct,
    key_schema: &'a DebeziumSchemaStruct,
) -> Result<(Schema, HashMap<String, &'a DebeziumSchemaStruct>), DebeziumSchemaError> {
    let pk_fields = match &key_schema.fields {
        None => vec![],
        Some(fields) => fields.iter().map(|f| f.field.clone().unwrap()).collect(),
    };

    match &schema.fields {
        None => Err(SchemaDefinitionNotFound),
        Some(fields) => {
            let new_schema_struct = fields.iter().find(|f| {
                if let Some(val) = f.field.clone() {
                    val == *"after"
                } else {
                    false
                }
            });

            if let Some(schema) = new_schema_struct {
                let mut pk_keys_indexes = vec![];
                let mut fields_schema_map: HashMap<String, &DebeziumSchemaStruct> = HashMap::new();

                let defined_fields: Result<Vec<FieldDefinition>, _> = match &schema.fields {
                    None => Ok(vec![]),
                    Some(fields) => fields
                        .iter()
                        .enumerate()
                        .map(|(idx, f)| {
                            let typ = map_type(f)?;
                            let name = f.field.clone().unwrap();
                            if pk_fields.contains(&name) {
                                pk_keys_indexes.push(idx);
                            }
                            fields_schema_map.insert(name.clone(), f);
                            Ok(FieldDefinition {
                                name,
                                typ,
                                nullable: f.optional,
                            })
                        })
                        .collect(),
                };

                Ok((
                    Schema {
                        identifier: Some(SchemaIdentifier { id: 1, version: 1 }),
                        fields: defined_fields?,
                        values: vec![],
                        primary_index: pk_keys_indexes,
                    },
                    fields_schema_map,
                ))
            } else {
                Err(SchemaDefinitionNotFound)
            }
        }
    }
}
