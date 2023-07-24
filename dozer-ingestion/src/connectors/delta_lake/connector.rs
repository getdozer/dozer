use crate::connectors::delta_lake::reader::DeltaLakeReader;
use crate::connectors::delta_lake::schema_helper::SchemaHelper;
use crate::connectors::delta_lake::ConnectorResult;
use crate::connectors::{
    table_name, Connector, ListOrFilterColumns, SourceSchemaResult, TableIdentifier, TableInfo,
};
use crate::errors::ConnectorError;
use crate::ingestion::Ingestor;
use dozer_types::ingestion_types::DeltaLakeConfig;
use tonic::async_trait;

#[derive(Debug)]
pub struct DeltaLakeConnector {
    config: DeltaLakeConfig,
}

impl DeltaLakeConnector {
    pub fn new(config: DeltaLakeConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Connector for DeltaLakeConnector {
    fn types_mapping() -> Vec<(String, Option<dozer_types::types::FieldType>)>
    where
        Self: Sized,
    {
        todo!()
    }

    async fn validate_connection(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn list_tables(&self) -> Result<Vec<TableIdentifier>, ConnectorError> {
        Ok(self
            .config
            .tables
            .iter()
            .map(|table| TableIdentifier::from_table_name(table.name.clone()))
            .collect())
    }

    async fn validate_tables(&self, tables: &[TableIdentifier]) -> Result<(), ConnectorError> {
        let mut delta_table_names = vec![];
        // Collect delta table names in config, the validate table info
        for delta_table in self.config.tables.iter() {
            delta_table_names.push(delta_table.name.as_str());
        }
        for table in tables.iter() {
            if !delta_table_names.contains(&table.name.as_str()) || table.schema.is_some() {
                return Err(ConnectorError::TableNotFound(table_name(
                    table.schema.as_deref(),
                    &table.name,
                )));
            }
        }
        Ok(())
    }

    async fn list_columns(
        &self,
        tables: Vec<TableIdentifier>,
    ) -> Result<Vec<TableInfo>, ConnectorError> {
        let table_infos = tables
            .into_iter()
            .map(|table| ListOrFilterColumns {
                schema: table.schema,
                name: table.name,
                columns: None,
            })
            .collect::<Vec<_>>();
        let schema_helper = SchemaHelper::new(self.config.clone());
        let source_schemas = schema_helper.get_schemas(&table_infos).await?;

        let mut result = vec![];
        for (source_schema, table_info) in source_schemas.into_iter().zip(table_infos) {
            let column_names = source_schema?
                .schema
                .fields
                .into_iter()
                .map(|field| field.name)
                .collect();
            result.push(TableInfo {
                schema: None,
                name: table_info.name,
                column_names,
            })
        }
        Ok(result)
    }

    async fn get_schemas(
        &self,
        table_infos: &[TableInfo],
    ) -> ConnectorResult<Vec<SourceSchemaResult>> {
        let table_infos = table_infos
            .iter()
            .map(|table_info| ListOrFilterColumns {
                schema: None,
                name: table_info.name.clone(),
                columns: Some(table_info.column_names.clone()),
            })
            .collect::<Vec<_>>();
        let schema_helper = SchemaHelper::new(self.config.clone());
        schema_helper.get_schemas(&table_infos).await
    }

    async fn start(&self, ingestor: &Ingestor, tables: Vec<TableInfo>) -> ConnectorResult<()> {
        let reader = DeltaLakeReader::new(self.config.clone());
        reader.read(&tables, ingestor).await
    }
}
