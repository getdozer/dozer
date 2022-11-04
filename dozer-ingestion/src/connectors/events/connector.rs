use crossbeam::channel::{unbounded, Receiver};

use crate::{
    connectors::{Connector, TableInfo},
    errors::ConnectorError,
};

// Initialize with a set of initial schemas.
pub struct EventsConfig<'a> {
    pub name: &'a str,
    pub schemas: Vec<(String, dozer_types::types::Schema)>,
}

pub struct EventsConnector<'a> {
    pub id: u64,
    config: EventsConfig<'a>,
}

impl<'a> EventsConnector<'a> {
    pub fn new(id: u64, config: EventsConfig<'a>) -> Self {
        Self { id, config }
    }

    pub fn add_schema(&mut self, schema_tuple: (String, dozer_types::types::Schema)) {
        self.config.schemas.push(schema_tuple);
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
        todo!()
    }

    fn initialize(
        &mut self,
        ingestor: std::sync::Arc<dozer_types::parking_lot::RwLock<crate::ingestion::Ingestor>>,
        tables: Option<Vec<TableInfo>>,
    ) -> Result<(), ConnectorError> {
        todo!()
    }

    fn start(&self) -> Result<(), ConnectorError> {
        todo!()
    }
}
