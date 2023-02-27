use crate::connectors::delta_lake::reader::DeltaLakeReader;
use crate::connectors::delta_lake::schema_helper::SchemaHelper;
use crate::connectors::delta_lake::ConnectorResult;
use crate::connectors::{Connector, TableInfo, ValidationResults};
use crate::ingestion::Ingestor;
use dozer_types::ingestion_types::DeltaLakeConfig;
use dozer_types::types::SourceSchema;
use std::collections::HashMap;

#[derive(Debug)]
pub struct DeltaLakeConnector {
    pub id: u64,
    config: DeltaLakeConfig,
}

impl DeltaLakeConnector {
    pub fn new(id: u64, config: DeltaLakeConfig) -> Self {
        Self { id, config }
    }
}

impl Connector for DeltaLakeConnector {
    fn validate(&self, _tables: Option<Vec<TableInfo>>) -> ConnectorResult<()> {
        Ok(())
    }

    fn validate_schemas(&self, _tables: &[TableInfo]) -> ConnectorResult<ValidationResults> {
        Ok(HashMap::new())
    }

    fn get_schemas(
        &self,
        table_names: Option<Vec<TableInfo>>,
    ) -> ConnectorResult<Vec<SourceSchema>> {
        let schema_helper = SchemaHelper::new(self.config.clone());
        schema_helper.get_schemas(table_names)
    }

    fn can_start_from(&self, _last_checkpoint: (u64, u64)) -> ConnectorResult<bool> {
        Ok(false)
    }

    fn start(
        &self,
        _last_checkpoint: Option<(u64, u64)>,
        ingestor: &Ingestor,
        tables: Option<Vec<TableInfo>>,
    ) -> ConnectorResult<()> {
        let tables = match tables {
            Some(tables) if !tables.is_empty() => tables,
            _ => return Ok(()),
        };

        let reader = DeltaLakeReader::new(self.config.clone());
        reader.read(&tables, ingestor)
    }

    fn get_tables(&self, _tables: Option<&[TableInfo]>) -> ConnectorResult<Vec<TableInfo>> {
        todo!()
    }
}
