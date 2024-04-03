use dozer_ingestion_connector::{
    dozer_types::types::{FieldDefinition, FieldType, Schema, SourceDefinition},
    CdcType, SourceSchema,
};

use crate::KafkaError;

pub struct NoSchemaRegistryBasic {}

impl NoSchemaRegistryBasic {
    pub fn get_single_schema() -> SourceSchema {
        let schema = Schema {
            fields: vec![
                FieldDefinition {
                    name: "key".to_string(),
                    typ: FieldType::String,
                    nullable: false,
                    source: SourceDefinition::Dynamic,
                    description: None,
                },
                FieldDefinition {
                    name: "message".to_string(),
                    typ: FieldType::String,
                    nullable: true,
                    source: SourceDefinition::Dynamic,
                    description: None,
                },
            ],
            primary_index: vec![0],
        };

        SourceSchema::new(schema, CdcType::FullChanges)
    }

    pub fn get_schema(table_names: Option<&[String]>) -> Result<Vec<SourceSchema>, KafkaError> {
        let mut schemas = vec![];
        if let Some(tables) = table_names {
            for _ in 0..tables.len() {
                let schema = Self::get_single_schema();
                schemas.push(schema);
            }
        }

        Ok(schemas)
    }
}
