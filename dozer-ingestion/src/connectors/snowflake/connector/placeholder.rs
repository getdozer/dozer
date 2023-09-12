use dozer_types::ingestion_types::SnowflakeConfig;
use tonic::async_trait;

use crate::{
    connectors::{Connector, SourceSchemaResult, TableIdentifier, TableInfo, TableToIngest},
    errors::ConnectorError,
};

#[derive(Debug)]
pub struct PlaceHolderSnowflakeConnector;

impl PlaceHolderSnowflakeConnector {
    pub fn new(_name: String, _config: SnowflakeConfig) -> Self {
        Self
    }
}

#[async_trait]
impl Connector for PlaceHolderSnowflakeConnector {
    fn types_mapping() -> Vec<(String, Option<dozer_types::types::FieldType>)>
    where
        Self: Sized,
    {
        todo!()
    }

    async fn validate_connection(&self) -> Result<(), ConnectorError> {
        todo!()
    }

    async fn list_tables(&self) -> Result<Vec<TableIdentifier>, ConnectorError> {
        todo!()
    }

    async fn validate_tables(&self, _tables: &[TableIdentifier]) -> Result<(), ConnectorError> {
        todo!()
    }

    async fn list_columns(
        &self,
        _tables: Vec<TableIdentifier>,
    ) -> Result<Vec<TableInfo>, ConnectorError> {
        todo!()
    }

    async fn get_schemas(
        &self,
        _table_infos: &[TableInfo],
    ) -> Result<Vec<SourceSchemaResult>, ConnectorError> {
        todo!()
    }

    async fn start(
        &self,
        _ingestor: &crate::ingestion::Ingestor,
        _tables: Vec<TableToIngest>,
    ) -> Result<(), ConnectorError> {
        todo!()
    }
}
