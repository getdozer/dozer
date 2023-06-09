use crate::errors::ConnectorError;
use crate::ingestion::Ingestor;

use crate::connectors::TableInfo;
use rdkafka::consumer::BaseConsumer;
use tonic::async_trait;

#[async_trait]
pub trait StreamConsumer {
    async fn run(
        &self,
        con: BaseConsumer,
        ingestor: &Ingestor,
        tables: Vec<TableInfo>,
        schema_registry_url: &Option<String>,
    ) -> Result<(), ConnectorError>;
}
