#![allow(clippy::type_complexity)]

use crate::connectors::kafka::debezium::stream_consumer::DebeziumSchemaStruct;
use crate::connectors::{CdcType, SourceSchema};

use crate::errors::{ConnectorError, DebeziumError};

use dozer_types::types::{FieldDefinition, Schema, SchemaIdentifier, SourceDefinition};

use crate::connectors::kafka::debezium::schema_registry::SchemaRegistry;
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use std::collections::HashMap;

pub struct SchemaRegistryBasic {}

impl SchemaRegistryBasic {
    pub async fn get_single_schema(
        id: u32,
        table_name: &str,
        schema_registry_url: &str,
    ) -> Result<(SourceSchema, HashMap<String, DebeziumSchemaStruct>), ConnectorError> {
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

        let defined_fields: Result<Vec<FieldDefinition>, ConnectorError> = fields
            .iter()
            .to_owned()
            .enumerate()
            .map(|(idx, f)| {
                let (typ, nullable) = SchemaRegistry::map_typ(f).map_err(|e| {
                    ConnectorError::DebeziumError(DebeziumError::DebeziumSchemaError(e))
                })?;
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
                })
            })
            .collect();

        let schema = Schema {
            identifier: Some(SchemaIdentifier { id, version: 1 }),
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
    ) -> Result<Vec<SourceSchema>, ConnectorError> {
        let mut schemas = vec![];
        if let Some(tables) = table_names {
            for (index, table_name) in tables.iter().enumerate() {
                let (schema, _) =
                    Self::get_single_schema(index as u32, table_name, &schema_registry_url).await?;
                schemas.push(schema);
            }
        }

        Ok(schemas)
    }
}
