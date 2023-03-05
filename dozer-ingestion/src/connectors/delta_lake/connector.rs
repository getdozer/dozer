use crate::connectors::delta_lake::reader::DeltaLakeReader;
use crate::connectors::delta_lake::schema_helper::SchemaHelper;
use crate::connectors::delta_lake::ConnectorResult;
use crate::connectors::{Connector, TableInfo, ValidationResults};
use crate::errors::ConnectorError;
use crate::errors::ConnectorError::TableNotFound;
use crate::ingestion::Ingestor;
use dozer_types::ingestion_types::DeltaLakeConfig;
use dozer_types::types::SourceSchema;

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
    fn validate(&self, tables: Option<Vec<TableInfo>>) -> ConnectorResult<()> {
        if tables.is_none() {
            return Ok(());
        }
        let mut delta_table_names = vec![];
        // Collect delta table names in config, the validate table info
        for delta_table in self.config.tables.iter() {
            delta_table_names.push(delta_table.name.as_str());
        }
        for table_info in tables.unwrap().iter() {
            if !delta_table_names.contains(&table_info.name.as_str()) {
                return Err(ConnectorError::TableNotFound(format!(
                    "{:?} not find in config",
                    table_info.name
                )));
            }
        }
        Ok(())
    }

    fn validate_schemas(&self, tables: &[TableInfo]) -> ConnectorResult<ValidationResults> {
        let schemas = self.get_schemas(Some(tables.to_vec()))?;
        let mut validation_result = ValidationResults::new();
        let existing_schemas_names: Vec<String> = schemas.iter().map(|s| s.name.clone()).collect();
        for table in tables {
            let mut result = vec![];
            if !existing_schemas_names.contains(&table.table_name) {
                result.push((None, Err(TableNotFound(table.table_name.clone()))));
            }

            validation_result.insert(table.name.clone(), result);
        }

        Ok(validation_result)
    }

    fn get_schemas(
        &self,
        table_names: Option<Vec<TableInfo>>,
    ) -> ConnectorResult<Vec<SourceSchema>> {
        let schema_helper = SchemaHelper::new(self.config.clone());
        schema_helper.get_schemas(self.id, table_names)
    }

    fn can_start_from(&self, _last_checkpoint: (u64, u64)) -> ConnectorResult<bool> {
        Ok(false)
    }

    fn start(
        &self,
        _last_checkpoint: Option<(u64, u64)>,
        ingestor: &Ingestor,
        tables: Vec<TableInfo>,
    ) -> ConnectorResult<()> {
        if tables.is_empty() {
            return Ok(());
        }

        let reader = DeltaLakeReader::new(self.config.clone());
        reader.read(&tables, ingestor)
    }

    fn get_tables(&self, tables: Option<&[TableInfo]>) -> ConnectorResult<Vec<TableInfo>> {
        self.get_tables_default(tables)
    }
}
