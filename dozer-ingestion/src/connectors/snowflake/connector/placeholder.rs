use dozer_types::ingestion_types::SnowflakeConfig;

use crate::{
    connectors::{Connector, SourceSchemaResult, TableIdentifier, TableInfo},
    errors::ConnectorError,
};

#[derive(Debug)]
pub struct PlaceHolderSnowflakeConnector;

impl PlaceHolderSnowflakeConnector {
    pub fn new(_name: String, _config: SnowflakeConfig) -> Self {
        Self
    }
}

impl Connector for PlaceHolderSnowflakeConnector {
    fn types_mapping() -> Vec<(String, Option<dozer_types::types::FieldType>)>
    where
        Self: Sized,
    {
        todo!()
    }

    fn validate_connection(&self) -> Result<(), ConnectorError> {
        todo!()
    }

    fn list_tables(&self) -> Result<Vec<TableIdentifier>, ConnectorError> {
        todo!()
    }

    fn validate_tables(&self, _tables: &[TableIdentifier]) -> Result<(), ConnectorError> {
        todo!()
    }

    fn list_columns(
        &self,
        _tables: Vec<TableIdentifier>,
    ) -> Result<Vec<TableInfo>, ConnectorError> {
        todo!()
    }

    fn get_schemas(
        &self,
        _table_infos: &[TableInfo],
    ) -> Result<Vec<SourceSchemaResult>, ConnectorError> {
        todo!()
    }

    fn start(
        &self,
        _ingestor: &crate::ingestion::Ingestor,
        _tables: Vec<TableInfo>,
    ) -> Result<(), ConnectorError> {
        todo!()
    }
}
