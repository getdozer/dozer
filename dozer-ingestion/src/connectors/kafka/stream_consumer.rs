use crate::errors::ConnectorError;
use crate::ingestion::Ingestor;
use kafka::consumer::Consumer;
use tonic::async_trait;

#[async_trait]
pub trait StreamConsumer {
    async fn run(
        &self,
        con: Consumer,
        ingestor: &Ingestor,
        topic: &str,
        schema_registry_url: &Option<String>,
    ) -> Result<(), ConnectorError>;
}
