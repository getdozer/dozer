use crate::{
    connectors::TableInfo,
    errors::{ConnectorError, ObjectStoreConnectorError},
};

use dozer_types::types::Operation;
use tokio::sync::mpsc::Sender;
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
        sender: Sender<Result<Option<Operation>, ObjectStoreConnectorError>>,
    ) -> Result<(), ConnectorError> {
        self.ingest(id, table, sender.clone()).await?;
        Ok(())
    }

    async fn snapshot(
        &self,
        id: usize,
        table: &TableInfo,
        sender: Sender<Result<Option<Operation>, ObjectStoreConnectorError>>,
    ) -> Result<(), ConnectorError>;

    async fn ingest(
        &self,
        id: usize,
        table: &TableInfo,
        sender: Sender<Result<Option<Operation>, ObjectStoreConnectorError>>,
    ) -> Result<(), ConnectorError>;
}
