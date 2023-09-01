use crate::errors::ConnectorError;
use crate::ingestion::Ingestor;

use crate::connectors::TableInfo;
use rdkafka::ClientConfig;
use tonic::async_trait;

#[async_trait]
pub trait StreamConsumer {
    async fn run(
        &self,
        client_config: ClientConfig,
        ingestor: &Ingestor,
        tables: Vec<TableInfo>,
        schema_registry_url: &Option<String>,
    ) -> Result<(), ConnectorError>;
}
