use std::sync::{atomic::AtomicBool, Arc};

use dozer_types::{ingestion_types::IngestionMessage, parking_lot::RwLock};

use crate::{
    connectors::{Connector, TableInfo},
    errors::ConnectorError,
    ingestion::Ingestor,
};

pub struct EventsConnector {
    pub id: u64,
    pub name: String,
    ingestor: Option<Arc<RwLock<Ingestor>>>,
}

impl EventsConnector {
    pub fn new(id: u64, name: String) -> Self {
        Self {
            id,
            name,
            ingestor: None,
        }
    }

    pub fn push(&mut self, msg: IngestionMessage) -> Result<(), ConnectorError> {
        let ingestor = self
            .ingestor
            .as_ref()
            .map_or(Err(ConnectorError::InitializationError), Ok)?;

        ingestor
            .write()
            .handle_message((self.id, msg))
            .map_err(ConnectorError::IngestorError)
    }
}

impl Connector for EventsConnector {
    fn get_schemas(
        &self,
        _table_names: Option<Vec<String>>,
    ) -> Result<Vec<(String, dozer_types::types::Schema)>, ConnectorError> {
        Ok(vec![])
    }

    fn get_tables(&self) -> Result<Vec<TableInfo>, ConnectorError> {
        Ok(vec![])
    }

    fn stop(&self) {}

    fn test_connection(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn initialize(
        &mut self,
        ingestor: std::sync::Arc<dozer_types::parking_lot::RwLock<crate::ingestion::Ingestor>>,
        _: Option<Vec<TableInfo>>,
    ) -> Result<(), ConnectorError> {
        self.ingestor = Some(ingestor);
        Ok(())
    }

    fn start(&self, _: Arc<AtomicBool>) -> Result<(), ConnectorError> {
        Ok(())
    }
}
