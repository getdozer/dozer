#![allow(clippy::type_complexity)]

use crate::connectors::{CdcType, SourceSchema};

use crate::errors::ConnectorError;

use dozer_types::types::{FieldDefinition, FieldType, Schema, SchemaIdentifier, SourceDefinition};

pub struct NoSchemaRegistryBasic {}

impl NoSchemaRegistryBasic {
    pub fn get_single_schema() -> SourceSchema {
        let schema = Schema {
            identifier: Some(SchemaIdentifier { id: 1, version: 1 }),
            fields: vec![
                FieldDefinition {
                    name: "key".to_string(),
                    typ: FieldType::String,
                    nullable: false,
                    source: SourceDefinition::Dynamic,
                },
                FieldDefinition {
                    name: "message".to_string(),
                    typ: FieldType::String,
                    nullable: true,
                    source: SourceDefinition::Dynamic,
                },
            ],
            primary_index: vec![0],
        };

        SourceSchema::new(schema, CdcType::FullChanges)
    }

    pub fn get_schema(table_names: Option<&[String]>) -> Result<Vec<SourceSchema>, ConnectorError> {
        match table_names {
            None => Ok(vec![]),
            Some(tables) => match tables.get(0) {
                None => Ok(vec![]),
                Some(_table) => {
                    let schema = Self::get_single_schema();

                    Ok(vec![schema])
                }
            },
        }
    }
}
