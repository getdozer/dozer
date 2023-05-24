use crate::connectors::object_store::schema_mapper::map_schema;
use crate::connectors::{CdcType, ListOrFilterColumns, SourceSchema, SourceSchemaResult};
use deltalake::arrow::datatypes::SchemaRef;
use deltalake::datafusion::prelude::SessionContext;
use dozer_types::ingestion_types::DeltaLakeConfig;
use std::sync::Arc;

use super::reader::table_path;
use super::ConnectorResult;

pub struct SchemaHelper {
    config: DeltaLakeConfig,
}

impl SchemaHelper {
    pub fn new(config: DeltaLakeConfig) -> Self {
        Self { config }
    }

    pub async fn get_schemas(
        &self,
        id: u32,
        tables: &[ListOrFilterColumns],
    ) -> ConnectorResult<Vec<SourceSchemaResult>> {
        let mut schemas = vec![];
        for table in tables.iter() {
            schemas.push(self.get_schemas_impl(id, table).await);
        }
        Ok(schemas)
    }

    pub async fn get_schemas_impl(
        &self,
        id: u32,
        table: &ListOrFilterColumns,
    ) -> ConnectorResult<SourceSchema> {
        let table_path = table_path(&self.config, &table.name)?;
        let ctx = SessionContext::new();
        let delta_table = deltalake::open_table(table_path).await?;
        let arrow_schema: SchemaRef = (*ctx.read_table(Arc::new(delta_table))?.schema())
            .clone()
            .into();
        let schema = map_schema(id, arrow_schema, table)?;
        Ok(SourceSchema::new(schema, CdcType::Nothing))
    }
}
