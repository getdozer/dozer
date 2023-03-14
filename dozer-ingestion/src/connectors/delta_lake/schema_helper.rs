use crate::connectors::delta_lake::reader::table_path;
use crate::connectors::delta_lake::ConnectorResult;
use crate::connectors::object_store::schema_mapper::{map_schema, TableInfo};
use crate::connectors::{CdcType, SourceSchema, SourceSchemaResult};
use deltalake::arrow::datatypes::SchemaRef;
use deltalake::datafusion::prelude::SessionContext;
use dozer_types::ingestion_types::DeltaLakeConfig;
use std::sync::Arc;
use tokio::runtime::Runtime;

pub struct SchemaHelper {
    config: DeltaLakeConfig,
}

impl SchemaHelper {
    pub fn new(config: DeltaLakeConfig) -> Self {
        Self { config }
    }

    pub fn get_schemas(
        &self,
        id: u64,
        tables: &[TableInfo],
    ) -> ConnectorResult<Vec<SourceSchemaResult>> {
        let mut schemas = vec![];
        let runtime = Runtime::new()?;
        for table in tables.iter() {
            schemas.push(runtime.block_on(self.get_schemas_impl(id, table)));
        }
        Ok(schemas)
    }

    async fn get_schemas_impl(&self, id: u64, table: &TableInfo) -> ConnectorResult<SourceSchema> {
        let table_path = table_path(&self.config, &table.name)?;
        let ctx = SessionContext::new();
        let delta_table = deltalake::open_table(table_path).await?;
        let arrow_schema: SchemaRef = (*ctx.read_table(Arc::new(delta_table))?.schema())
            .clone()
            .into();
        let schema = map_schema(id as u32, arrow_schema, table)?;
        Ok(SourceSchema::new(schema, CdcType::Nothing))
    }
}
