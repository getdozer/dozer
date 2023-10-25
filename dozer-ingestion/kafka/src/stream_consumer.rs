use crate::KafkaError;

use dozer_ingestion_connector::{async_trait, Ingestor, TableToIngest};
use rdkafka::ClientConfig;

#[async_trait]
pub trait StreamConsumer {
    async fn run(
        &self,
        client_config: ClientConfig,
        ingestor: &Ingestor,
        tables: Vec<TableToIngest>,
        schema_registry_url: &Option<String>,
    ) -> Result<(), KafkaError>;
}
