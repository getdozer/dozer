use std::sync::Arc;

use dozer_types::{ingestion_types::IngestionMessage, parking_lot::RwLock};

use crate::{
    connectors::{Connector, TableInfo},
    errors::ConnectorError,
    ingestion::Ingestor,
};

// Initialize with a set of schemas.
pub struct EventsConfig<'a> {
    pub name: &'a str,
    pub schemas: Vec<(String, dozer_types::types::Schema)>,
}

pub struct EventsConnector<'a> {
    pub id: u64,
    config: EventsConfig<'a>,
    ingestor: Option<Arc<RwLock<Ingestor>>>,
}

impl<'a> EventsConnector<'a> {
    pub fn new(id: u64, config: EventsConfig<'a>) -> Self {
        Self {
            id,
            config,
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

impl<'a> Connector for EventsConnector<'a> {
    fn get_schemas(&self) -> Result<Vec<(String, dozer_types::types::Schema)>, ConnectorError> {
        Ok(self.config.schemas.to_owned())
    }

    fn get_tables(&self) -> Result<Vec<TableInfo>, ConnectorError> {
        Err(ConnectorError::UnsupportedConnectorMethod(
            "get_tables".to_string(),
        ))
    }

    fn stop(&self) {}

    fn test_connection(&self) -> Result<(), ConnectorError> {
        Err(ConnectorError::UnsupportedConnectorMethod(
            "test_connection".to_string(),
        ))
    }

    fn initialize(
        &mut self,
        ingestor: std::sync::Arc<dozer_types::parking_lot::RwLock<crate::ingestion::Ingestor>>,
        _: Option<Vec<TableInfo>>,
    ) -> Result<(), ConnectorError> {
        self.ingestor = Some(ingestor);
        Ok(())
    }

    fn start(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}
