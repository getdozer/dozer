use deltalake::{arrow::datatypes::SchemaRef, datafusion::prelude::SessionContext};
use dozer_ingestion_connector::{
    dozer_types::{errors::internal::BoxedError, models::ingestion_types::DeltaLakeConfig},
    utils::ListOrFilterColumns,
    CdcType, SourceSchema, SourceSchemaResult,
};
use dozer_ingestion_object_store::schema_mapper::map_schema;

use crate::reader::table_path;
use std::sync::Arc;

pub struct SchemaHelper {
    config: DeltaLakeConfig,
}

impl SchemaHelper {
    pub fn new(config: DeltaLakeConfig) -> Self {
        Self { config }
    }

    pub async fn get_schemas(
        &self,
        tables: &[ListOrFilterColumns],
    ) -> Result<Vec<SourceSchemaResult>, BoxedError> {
        let mut schemas = vec![];
        for table in tables.iter() {
            schemas.push(self.get_schemas_impl(table).await);
        }
        Ok(schemas)
    }

    async fn get_schemas_impl(
        &self,
        table: &ListOrFilterColumns,
    ) -> Result<SourceSchema, BoxedError> {
        let table_path = table_path(&self.config, &table.name)?;
        let ctx = SessionContext::new();
        let delta_table = deltalake::open_table(table_path).await?;
        let arrow_schema: SchemaRef = (*ctx.read_table(Arc::new(delta_table))?.schema())
            .clone()
            .into();
        let schema = map_schema(arrow_schema, table)?;
        Ok(SourceSchema::new(schema, CdcType::Nothing))
    }
}
