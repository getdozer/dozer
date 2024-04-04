use crate::reader::DeltaLakeReader;
use crate::schema_helper::SchemaHelper;
use dozer_ingestion_connector::{
    async_trait,
    dozer_types::{
        errors::internal::BoxedError, models::ingestion_types::DeltaLakeConfig, node::SourceState,
        types::FieldType,
    },
    utils::{ListOrFilterColumns, TableNotFound},
    Connector, Ingestor, SourceSchemaResult, TableIdentifier, TableInfo,
};

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
    fn types_mapping() -> Vec<(String, Option<FieldType>)>
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
            .config
            .tables
            .iter()
            .map(|table| TableIdentifier::from_table_name(table.name.clone()))
            .collect())
    }

    async fn validate_tables(&mut self, tables: &[TableIdentifier]) -> Result<(), BoxedError> {
        let mut delta_table_names = vec![];
        // Collect delta table names in config, the validate table info
        for delta_table in self.config.tables.iter() {
            delta_table_names.push(delta_table.name.as_str());
        }
        for table in tables.iter() {
            if !delta_table_names.contains(&table.name.as_str()) || table.schema.is_some() {
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
        &mut self,
        table_infos: &[TableInfo],
    ) -> Result<Vec<SourceSchemaResult>, BoxedError> {
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

    async fn serialize_state(&self) -> Result<Vec<u8>, BoxedError> {
        Ok(vec![])
    }

    async fn start(
        &mut self,
        ingestor: &Ingestor,
        tables: Vec<TableInfo>,
        last_checkpoint: SourceState,
    ) -> Result<(), BoxedError> {
        assert!(last_checkpoint.op_id().is_none());
        let reader = DeltaLakeReader::new(self.config.clone());
        reader.read(&tables, ingestor).await
    }
}
