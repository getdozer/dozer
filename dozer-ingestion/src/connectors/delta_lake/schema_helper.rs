use crate::connectors::delta_lake::reader::table_path;
use crate::connectors::delta_lake::ConnectorResult;
use crate::connectors::object_store::schema_mapper::map_schema;
use crate::connectors::{CdcType, ListOrFilterColumns, SourceSchema, SourceSchemaResult};
use deltalake::arrow::datatypes::SchemaRef;
use deltalake::datafusion::prelude::SessionContext;
use dozer_types::ingestion_types::DeltaLakeConfig;
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
    ) -> ConnectorResult<Vec<SourceSchemaResult>> {
        let mut schemas = vec![];
        for table in tables.iter() {
            schemas.push(self.get_schemas_impl(table).await);
        }
        Ok(schemas)
    }

    async fn get_schemas_impl(&self, table: &ListOrFilterColumns) -> ConnectorResult<SourceSchema> {
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
