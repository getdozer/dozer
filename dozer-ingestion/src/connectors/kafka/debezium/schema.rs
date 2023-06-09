use dozer_types::serde_json::Value;
use std::collections::HashMap;

use crate::connectors::kafka::debezium::stream_consumer::DebeziumSchemaStruct;

use crate::errors::KafkaSchemaError;
use crate::errors::KafkaSchemaError::{SchemaDefinitionNotFound, TypeNotSupported};
use dozer_types::types::{FieldDefinition, FieldType, Schema, SchemaIdentifier, SourceDefinition};

// Reference: https://debezium.io/documentation/reference/0.9/connectors/postgresql.html
pub fn map_type(schema: &DebeziumSchemaStruct) -> Result<FieldType, KafkaSchemaError> {
    match schema.name.clone() {
        None => match schema.r#type.clone() {
            Value::String(typ) => match typ.as_str() {
                "int" | "int8" | "int16" | "int32" | "int64" => Ok(FieldType::Int),
                "string" => Ok(FieldType::String),
                "bytes" => Ok(FieldType::Binary),
                "float" | "float32" | "float64" | "double" => Ok(FieldType::Float),
                "boolean" => Ok(FieldType::Boolean),
                _ => Err(TypeNotSupported(typ)),
            },
            _ => Err(TypeNotSupported("Unexpected value type".to_string())),
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
            "io.debezium.data.Json" => Ok(FieldType::Json),
            _ => Err(TypeNotSupported(name)),
        },
    }
}

pub fn map_schema(
    schema: &DebeziumSchemaStruct,
    key_schema: &DebeziumSchemaStruct,
) -> Result<(Schema, HashMap<String, DebeziumSchemaStruct>), KafkaSchemaError> {
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
                let mut fields_schema_map: HashMap<String, DebeziumSchemaStruct> = HashMap::new();

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
                            fields_schema_map.insert(name.clone(), f.clone());
                            Ok(FieldDefinition {
                                name,
                                typ,
                                nullable: f.optional.map_or(false, |o| o),
                                source: SourceDefinition::Dynamic,
                            })
                        })
                        .collect(),
                };

                Ok((
                    Schema {
                        identifier: Some(SchemaIdentifier { id: 1, version: 1 }),
                        fields: defined_fields?,
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

#[cfg(test)]
mod tests {
    use crate::connectors::kafka::debezium::schema::{map_schema, map_type};
    use crate::connectors::kafka::debezium::stream_consumer::DebeziumSchemaStruct;
    use crate::errors::KafkaSchemaError::SchemaDefinitionNotFound;
    use crate::errors::KafkaSchemaError::TypeNotSupported;
    use dozer_types::serde_json::Value;
    use dozer_types::types::{
        FieldDefinition, FieldType, Schema, SchemaIdentifier, SourceDefinition,
    };

    #[test]
    fn test_it_fails_when_schema_empty() {
        let schema = DebeziumSchemaStruct {
            r#type: Value::String("empty".to_string()),
            fields: None,
            optional: Some(false),
            name: None,
            field: None,
            version: None,
            parameters: None,
        };

        let key_schema = DebeziumSchemaStruct {
            r#type: Value::String("before".to_string()),
            fields: None,
            optional: Some(false),
            name: None,
            field: None,
            version: None,
            parameters: None,
        };

        let actual_error = map_schema(&schema, &key_schema).unwrap_err();
        assert_eq!(actual_error, SchemaDefinitionNotFound);
    }

    #[test]
    fn test_it_converts_schema() {
        let schema = DebeziumSchemaStruct {
            r#type: Value::String("empty".to_string()),
            fields: Some(vec![DebeziumSchemaStruct {
                r#type: Value::String("after".to_string()),
                fields: Some(vec![
                    DebeziumSchemaStruct {
                        r#type: Value::String("int32".to_string()),
                        fields: None,
                        optional: Some(false),
                        name: None,
                        field: Some("id".to_string()),
                        version: None,
                        parameters: None,
                    },
                    DebeziumSchemaStruct {
                        r#type: Value::String("string".to_string()),
                        fields: None,
                        optional: Some(true),
                        name: None,
                        field: Some("name".to_string()),
                        version: None,
                        parameters: None,
                    },
                ]),
                optional: Some(false),
                name: None,
                field: Some("after".to_string()),
                version: None,
                parameters: None,
            }]),
            optional: Some(false),
            name: None,
            field: Some("struct".to_string()),
            version: None,
            parameters: None,
        };

        let key_schema = DebeziumSchemaStruct {
            r#type: Value::String("-".to_string()),
            fields: Some(vec![DebeziumSchemaStruct {
                r#type: Value::String("int32".to_string()),
                fields: None,
                optional: Some(false),
                name: None,
                field: Some("id".to_string()),
                version: None,
                parameters: None,
            }]),
            optional: Some(false),
            name: None,
            field: None,
            version: None,
            parameters: None,
        };

        let (schema, _) = map_schema(&schema, &key_schema).unwrap();
        let expected_schema = Schema {
            identifier: Some(SchemaIdentifier { id: 1, version: 1 }),
            fields: vec![
                FieldDefinition {
                    name: "id".to_string(),
                    typ: FieldType::Int,
                    nullable: false,
                    source: SourceDefinition::Dynamic,
                },
                FieldDefinition {
                    name: "name".to_string(),
                    typ: FieldType::String,
                    nullable: true,
                    source: SourceDefinition::Dynamic,
                },
            ],
            primary_index: vec![0],
        };
        assert_eq!(schema, expected_schema);
    }

    #[test]
    fn test_it_converts_empty_schema() {
        let schema = DebeziumSchemaStruct {
            r#type: Value::String("empty".to_string()),
            fields: Some(vec![DebeziumSchemaStruct {
                r#type: Value::String("after".to_string()),
                fields: None,
                optional: Some(false),
                name: None,
                field: Some("after".to_string()),
                version: None,
                parameters: None,
            }]),
            optional: Some(false),
            name: None,
            field: Some("struct".to_string()),
            version: None,
            parameters: None,
        };

        let key_schema = DebeziumSchemaStruct {
            r#type: Value::String("-".to_string()),
            fields: Some(vec![]),
            optional: Some(false),
            name: None,
            field: None,
            version: None,
            parameters: None,
        };

        let (schema, _) = map_schema(&schema, &key_schema).unwrap();
        let expected_schema = Schema {
            identifier: Some(SchemaIdentifier { id: 1, version: 1 }),
            fields: vec![],
            primary_index: vec![],
        };
        assert_eq!(schema, expected_schema);
    }

    macro_rules! test_map_type {
        ($a:expr,$b:expr,$c:expr) => {
            let schema = DebeziumSchemaStruct {
                r#type: Value::String($a.to_string()),
                fields: None,
                optional: Some(false),
                name: $b,
                field: None,
                version: None,
                parameters: None,
            };

            let typ = map_type(&schema);
            assert_eq!(typ, $c);
        };
    }

    #[test]
    fn test_map_type() {
        test_map_type!("int8", None, Ok(FieldType::Int));
        test_map_type!("string", None, Ok(FieldType::String));
        test_map_type!("bytes", None, Ok(FieldType::Binary));
        test_map_type!("float32", None, Ok(FieldType::Float));
        test_map_type!("boolean", None, Ok(FieldType::Boolean));
        test_map_type!(
            "not found",
            None,
            Err(TypeNotSupported("not found".to_string()))
        );
        test_map_type!(
            "int8",
            Some("io.debezium.time.MicroTime".to_string()),
            Ok(FieldType::Timestamp)
        );
        test_map_type!(
            "int8",
            Some("io.debezium.time.Date".to_string()),
            Ok(FieldType::Date)
        );
        test_map_type!(
            "int8",
            Some("org.apache.kafka.connect.data.Decimal".to_string()),
            Ok(FieldType::Decimal)
        );
        test_map_type!(
            "string",
            Some("io.debezium.data.Json".to_string()),
            Ok(FieldType::Json)
        );
        test_map_type!(
            "string",
            Some("not existing".to_string()),
            Err(TypeNotSupported("not existing".to_string()))
        );
    }
}
