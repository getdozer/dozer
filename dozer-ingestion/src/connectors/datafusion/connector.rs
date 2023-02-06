use dozer_types::ingestion_types::DataFusionConfig;

use crate::connectors::Connector;

pub struct DataFusionConnector {
    pub id: u64,
    _config: DataFusionConfig,
}

impl DataFusionConnector {
    pub fn new(id: u64, _config: DataFusionConfig) -> Self {
        Self { id, _config }
    }
}

impl Connector for DataFusionConnector {
    fn get_schemas(
        &self,
        _table_names: Option<Vec<crate::connectors::TableInfo>>,
    ) -> Result<Vec<dozer_types::types::SchemaWithChangesType>, crate::errors::ConnectorError> {
        todo!()
    }

    fn get_tables(
        &self,
    ) -> Result<Vec<crate::connectors::TableInfo>, crate::errors::ConnectorError> {
        todo!()
    }

    fn test_connection(&self) -> Result<(), crate::errors::ConnectorError> {
        todo!()
    }

    fn initialize(
        &mut self,
        _ingestor: std::sync::Arc<dozer_types::parking_lot::RwLock<crate::ingestion::Ingestor>>,
        _tables: Option<Vec<crate::connectors::TableInfo>>,
    ) -> Result<(), crate::errors::ConnectorError> {
        todo!()
    }

    fn start(&self, _from_seq: Option<(u64, u64)>) -> Result<(), crate::errors::ConnectorError> {
        todo!()
    }

    fn stop(&self) {
        todo!()
    }

    fn validate(
        &self,
        _tables: Option<Vec<crate::connectors::TableInfo>>,
    ) -> Result<(), crate::errors::ConnectorError> {
        todo!()
    }

    fn validate_schemas(
        &self,
        _tables: &[crate::connectors::TableInfo],
    ) -> Result<crate::connectors::ValidationResults, crate::errors::ConnectorError> {
        todo!()
    }
}
