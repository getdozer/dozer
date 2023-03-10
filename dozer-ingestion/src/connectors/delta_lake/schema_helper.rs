use crate::connectors::delta_lake::reader::table_path;
use crate::connectors::delta_lake::ConnectorResult;
use crate::connectors::object_store::SchemaMapper;
use crate::connectors::TableInfo;
use deltalake::arrow::datatypes::SchemaRef;
use deltalake::datafusion::prelude::SessionContext;
use dozer_types::ingestion_types::DeltaLakeConfig;
use dozer_types::types::ReplicationChangesTrackingType::Nothing;
use dozer_types::types::SourceSchema;
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
        tables: Option<&Vec<TableInfo>>,
    ) -> ConnectorResult<Vec<SourceSchema>> {
        if tables.is_none() {
            return Ok(vec![]);
        }
        let tables = tables.unwrap();
        let mut schemas = vec![];
        let runtime = Runtime::new()?;
        for table in tables.iter() {
            let schema = runtime.block_on(self.get_schemas_impl(id, table))?;
            schemas.push(schema);
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
        let schema_mapper = SchemaMapper::new(self.config.clone());
        let schema = schema_mapper.map_schema(id as u32, arrow_schema, table)?;
        Ok(SourceSchema::new(table.name.clone(), schema, Nothing))
    }
}
