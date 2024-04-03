#![allow(clippy::type_complexity)]

use dozer_ingestion_connector::{
    dozer_types::types::{FieldDefinition, Schema, SourceDefinition},
    CdcType, SourceSchema,
};
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use std::collections::HashMap;

use crate::{
    debezium::{schema_registry::SchemaRegistry, stream_consumer::DebeziumSchemaStruct},
    KafkaError,
};

pub struct SchemaRegistryBasic {}

impl SchemaRegistryBasic {
    pub async fn get_single_schema(
        table_name: &str,
        schema_registry_url: &str,
    ) -> Result<(SourceSchema, HashMap<String, DebeziumSchemaStruct>), KafkaError> {
        let sr_settings = SrSettings::new(schema_registry_url.to_string());
        let key_result = SchemaRegistry::fetch_struct(&sr_settings, table_name, true).await?;
        let schema_result = SchemaRegistry::fetch_struct(&sr_settings, table_name, false).await?;

        let pk_fields = key_result.fields.map_or(vec![], |fields| {
            fields
                .iter()
                .map(|f| f.name.clone().map_or("".to_string(), |name| name))
                .collect()
        });

        let fields = schema_result.fields.map_or(vec![], |f| f);
        let mut pk_keys_indexes = vec![];
        let mut fields_schema_map: HashMap<String, DebeziumSchemaStruct> = HashMap::new();

        let defined_fields: Result<Vec<FieldDefinition>, KafkaError> = fields
            .iter()
            .to_owned()
            .enumerate()
            .map(|(idx, f)| {
                let (typ, nullable) =
                    SchemaRegistry::map_typ(f).map_err(KafkaError::KafkaSchemaError)?;
                let name = f.name.clone().unwrap();
                if pk_fields.contains(&name) {
                    pk_keys_indexes.push(idx);
                }
                fields_schema_map.insert(name.clone(), f.clone());
                Ok(FieldDefinition {
                    name,
                    typ,
                    nullable,
                    source: SourceDefinition::Dynamic,
                    description: None,
                })
            })
            .collect();

        let schema = Schema {
            fields: defined_fields?,
            primary_index: pk_keys_indexes,
        };

        Ok((
            SourceSchema::new(schema, CdcType::FullChanges),
            fields_schema_map,
        ))
    }

    pub async fn get_schema(
        table_names: Option<&[String]>,
        schema_registry_url: String,
    ) -> Result<Vec<SourceSchema>, KafkaError> {
        let mut schemas = vec![];
        if let Some(tables) = table_names {
            for table_name in tables.iter() {
                let (schema, _) = Self::get_single_schema(table_name, &schema_registry_url).await?;
                schemas.push(schema);
            }
        }

        Ok(schemas)
    }
}
