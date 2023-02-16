use std::collections::HashMap;

use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::types::ReplicationChangesTrackingType;

use crate::connectors::ValidationResults;
use crate::{
    connectors::{Connector, TableInfo},
    errors::ConnectorError,
    ingestion::Ingestor,
};

pub struct EventsConnector {
    pub id: u64,
    pub name: String,
}

impl EventsConnector {
    pub fn new(id: u64, name: String) -> Self {
        Self { id, name }
    }

    pub fn push(
        &mut self,
        ingestor: &Ingestor,
        msg: IngestionMessage,
    ) -> Result<(), ConnectorError> {
        ingestor
            .handle_message(((0, 0), msg))
            .map_err(ConnectorError::IngestorError)
    }
}

impl Connector for EventsConnector {
    fn get_schemas(
        &self,
        _table_names: Option<Vec<TableInfo>>,
    ) -> Result<
        Vec<(
            String,
            dozer_types::types::Schema,
            ReplicationChangesTrackingType,
        )>,
        ConnectorError,
    > {
        Ok(vec![])
    }

    fn start(
        &self,
        _from_seq: Option<(u64, u64)>,
        _ingestor: &Ingestor,
        _tables: Option<Vec<TableInfo>>,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn validate(&self, _tables: Option<Vec<TableInfo>>) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn validate_schemas(&self, _tables: &[TableInfo]) -> Result<ValidationResults, ConnectorError> {
        Ok(HashMap::new())
    }

    fn get_tables(&self, _tables: Option<&[TableInfo]>) -> Result<Vec<TableInfo>, ConnectorError> {
        todo!()
    }
}
