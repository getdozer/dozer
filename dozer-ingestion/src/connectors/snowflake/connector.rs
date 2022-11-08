use std::sync::Arc;

use crate::connectors::snowflake::connection::client::Client;
use crate::connectors::Connector;
use crate::ingestion::Ingestor;
use crate::{connectors::TableInfo, errors::ConnectorError};
use dozer_types::ingestion_types::SnowflakeConfig;
use dozer_types::parking_lot::RwLock;

use tokio::runtime::Runtime;

pub struct SnowflakeConnector {
    pub id: u64,
    config: SnowflakeConfig,
    ingestor: Option<Arc<RwLock<Ingestor>>>,
}

impl SnowflakeConnector {
    pub fn new(id: u64, config: SnowflakeConfig) -> Self {
        Self {
            id,
            config,
            ingestor: None,
        }
    }
}

impl Connector for SnowflakeConnector {
    fn get_schemas(&self) -> Result<Vec<(String, dozer_types::types::Schema)>, ConnectorError> {
        let client = Client::new(&self.config);
        client.execute("SELECT * FROM accounts;".to_string());
        Ok(vec![])
    }

    fn get_tables(&self) -> Result<Vec<TableInfo>, ConnectorError> {
        Ok(vec![])
    }

    fn test_connection(&self) -> Result<(), ConnectorError> {
        todo!()
    }

    fn initialize(
        &mut self,
        ingestor: Arc<RwLock<Ingestor>>,
        _: Option<Vec<TableInfo>>,
    ) -> Result<(), ConnectorError> {
        self.ingestor = Some(ingestor);
        Ok(())
    }

    fn start(&self) -> Result<(), ConnectorError> {
        let connector_id = self.id;
        let ingestor = self
            .ingestor
            .as_ref()
            .map_or(Err(ConnectorError::InitializationError), Ok)?
            .clone();
        Runtime::new()
            .unwrap()
            .block_on(async { run(self.config.clone(), ingestor, connector_id).await })
    }

    fn stop(&self) {}
}

async fn run(
    _config: SnowflakeConfig,
    _ingestor: Arc<RwLock<Ingestor>>,
    _connector_id: u64,
) -> Result<(), ConnectorError> {
    Ok(())
}
