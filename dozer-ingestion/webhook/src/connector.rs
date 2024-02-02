use crate::{server::WebhookServer, util::extract_source_schema, Error};
use dozer_ingestion_connector::{
    async_trait,
    dozer_types::{
        self, errors::internal::BoxedError, models::ingestion_types::WebhookConfig,
        node::OpIdentifier,
    },
    utils::TableNotFound,
    Connector, Ingestor, SourceSchema, SourceSchemaResult, TableIdentifier, TableInfo,
};
use std::{collections::HashMap, sync::Arc, vec};

#[derive(Debug)]
pub struct WebhookConnector {
    pub config: WebhookConfig,
}

impl WebhookConnector {
    pub fn new(config: WebhookConfig) -> Self {
        Self { config }
    }

    fn get_all_schemas(&self) -> Result<HashMap<String, SourceSchema>, Error> {
        let mut result: HashMap<String, SourceSchema> = HashMap::new();
        let config = &self.config;
        for endpoint in &config.endpoints {
            let schemas = extract_source_schema(endpoint.schema.clone());
            for (key, value) in schemas {
                result.insert(key, value);
            }
        }

        Ok(result)
    }
}

#[async_trait]
impl Connector for WebhookConnector {
    fn types_mapping() -> Vec<(String, Option<dozer_types::types::FieldType>)>
    where
        Self: Sized,
    {
        todo!()
    }

    async fn validate_connection(&mut self) -> Result<(), BoxedError> {
        Ok(())
    }

    async fn list_tables(&mut self) -> Result<Vec<TableIdentifier>, BoxedError> {
        Ok(self
            .get_all_schemas()?
            .into_keys()
            .map(TableIdentifier::from_table_name)
            .collect())
    }

    async fn validate_tables(&mut self, tables: &[TableIdentifier]) -> Result<(), BoxedError> {
        let schemas = self.get_all_schemas()?;
        for table in tables {
            if !schemas
                .iter()
                .any(|(name, _)| name == &table.name && table.schema.is_none())
            {
                return Err(TableNotFound {
                    schema: table.schema.clone(),
                    name: table.name.clone(),
                }
                .into());
            }
        }
        Ok(())
    }

    async fn list_columns(
        &mut self,
        tables: Vec<TableIdentifier>,
    ) -> Result<Vec<TableInfo>, BoxedError> {
        let schemas = self.get_all_schemas()?;
        let mut result: Vec<TableInfo> = vec![];
        for table in tables {
            let source_schema_option = schemas.get(table.name.as_str());
            match source_schema_option {
                Some(source_schema) => {
                    let column_names = source_schema
                        .schema
                        .fields
                        .iter()
                        .map(|field| field.name.clone())
                        .collect();
                    if result
                        .iter()
                        .any(|table_info| table_info.name == table.name)
                    {
                        continue;
                    }
                    result.push(TableInfo {
                        schema: table.schema,
                        name: table.name,
                        column_names,
                    })
                }
                None => {
                    return Err(TableNotFound {
                        schema: table.schema.clone(),
                        name: table.name.clone(),
                    }
                    .into());
                }
            }
        }
        Ok(result)
    }

    async fn get_schemas(
        &mut self,
        table_infos: &[TableInfo],
    ) -> Result<Vec<SourceSchemaResult>, BoxedError> {
        let schemas = self.get_all_schemas()?;
        let mut result = vec![];
        for table in table_infos {
            let table_name = table.name.clone();
            let schema = schemas.get(table_name.as_str());
            match schema {
                Some(schema) => {
                    result.push(Ok(schema.clone()));
                }
                None => {
                    result.push(Err(TableNotFound {
                        schema: table.schema.clone(),
                        name: table.name.clone(),
                    }
                    .into()));
                }
            }
        }
        Ok(result)
    }

    async fn serialize_state(&self) -> Result<Vec<u8>, BoxedError> {
        Ok(vec![])
    }

    async fn start(
        &mut self,
        ingestor: &Ingestor,
        tables: Vec<TableInfo>,
        _last_checkpoint: Option<OpIdentifier>,
    ) -> Result<(), BoxedError> {
        let config = self.config.clone();
        let server = WebhookServer::new(config);
        server
            .start(Arc::new(ingestor.to_owned()), tables)
            .await
            .map_err(Into::into)
    }
}
