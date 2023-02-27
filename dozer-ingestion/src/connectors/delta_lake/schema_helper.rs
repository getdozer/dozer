use crate::connectors::delta_lake::ConnectorResult;
use crate::connectors::TableInfo;
use dozer_types::ingestion_types::DeltaLakeConfig;
use dozer_types::types::SourceSchema;

pub struct SchemaHelper {
    config: DeltaLakeConfig,
}

impl SchemaHelper {
    pub fn new(config: DeltaLakeConfig) -> Self {
        Self { config }
    }
    pub fn get_schemas(
        &self,
        _table_names: Option<Vec<TableInfo>>,
    ) -> ConnectorResult<Vec<SourceSchema>> {
        todo!()
    }
}
