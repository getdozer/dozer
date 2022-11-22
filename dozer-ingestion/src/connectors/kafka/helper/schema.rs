use crate::connectors::kafka::connector::KafkaSchemaStruct;


use dozer_types::types::{FieldDefinition, FieldType, Schema, SchemaIdentifier};


fn map_type(typ: String) -> Option<FieldType> {
    match typ.as_str() {
        "int8" | "int16" | "int32" | "int64" => Some(FieldType::Int),
        "string" => Some(FieldType::String),
        _ => None,
    }
}

pub fn map_schema(schema: &KafkaSchemaStruct, key_schema: &KafkaSchemaStruct) -> Option<Schema> {
    let pk_fields = match &key_schema.fields {
        None => vec![],
        Some(fields) => fields.iter().map(|f| f.field.clone().unwrap()).collect(),
    };

    match &schema.fields {
        None => None,
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
                let defined_fields = match &schema.fields {
                    None => vec![],
                    Some(fields) => fields
                        .iter()
                        .enumerate()
                        .map(|(idx, f)| {
                            let typ = map_type(f.r#type.clone()).unwrap();
                            let name = f.field.clone().unwrap();
                            if pk_fields.contains(&name) {
                                pk_keys_indexes.push(idx);
                            }
                            FieldDefinition {
                                name,
                                typ,
                                nullable: f.optional,
                            }
                        })
                        .collect(),
                };

                Some(Schema {
                    identifier: Some(SchemaIdentifier { id: 1, version: 1 }),
                    fields: defined_fields,
                    values: vec![],
                    primary_index: pk_keys_indexes,
                    secondary_indexes: vec![],
                })
            } else {
                None
            }
        }
    }
}
