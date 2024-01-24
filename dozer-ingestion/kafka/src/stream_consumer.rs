use crate::KafkaError;

use dozer_ingestion_connector::{
    async_trait, dozer_types::node::OpIdentifier, Ingestor, TableInfo,
};
use rdkafka::ClientConfig;

#[async_trait]
pub trait StreamConsumer {
    async fn run(
        &self,
        client_config: ClientConfig,
        ingestor: &Ingestor,
        tables: Vec<TableInfo>,
        last_checkpoint: Option<OpIdentifier>,
        schema_registry_url: &Option<String>,
    ) -> Result<(), KafkaError>;
}
