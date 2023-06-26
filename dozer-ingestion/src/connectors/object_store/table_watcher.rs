use crate::{
    connectors::TableInfo,
    errors::{ConnectorError, ObjectStoreConnectorError},
};

use dozer_types::ingestion_types::IngestionMessageKind;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tonic::async_trait;

#[derive(Debug, Eq, Clone)]
pub struct FileInfo {
    pub name: String,
    pub last_modified: i64,
}

impl Ord for FileInfo {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.last_modified.cmp(&other.last_modified)
    }
}

impl PartialOrd for FileInfo {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for FileInfo {
    fn eq(&self, other: &Self) -> bool {
        self.last_modified == other.last_modified
    }
}

#[async_trait]
pub trait TableWatcher {
    async fn watch(
        &self,
        id: usize,
        table: &TableInfo,
        sender: Sender<Result<Option<IngestionMessageKind>, ObjectStoreConnectorError>>,
    ) -> Result<(), ConnectorError> {
        self.ingest(id, table, sender.clone()).await?;
        Ok(())
    }

    async fn snapshot(
        &self,
        id: usize,
        table: &TableInfo,
        sender: Sender<Result<Option<IngestionMessageKind>, ObjectStoreConnectorError>>,
    ) -> Result<JoinHandle<()>, ConnectorError>;

    async fn ingest(
        &self,
        id: usize,
        table: &TableInfo,
        sender: Sender<Result<Option<IngestionMessageKind>, ObjectStoreConnectorError>>,
    ) -> Result<(), ConnectorError>;
}
