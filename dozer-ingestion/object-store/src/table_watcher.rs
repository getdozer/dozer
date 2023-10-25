use std::collections::HashMap;

use dozer_ingestion_connector::{
    async_trait,
    dozer_types::{
        chrono::{DateTime, Utc},
        models::ingestion_types::IngestionMessage,
    },
    tokio::{sync::mpsc::Sender, task::JoinHandle},
    TableInfo,
};

use crate::ObjectStoreConnectorError;

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
        table_index: usize,
        table: &TableInfo,
        sender: Sender<Result<Option<IngestionMessage>, ObjectStoreConnectorError>>,
    ) -> Result<(), ObjectStoreConnectorError> {
        self.ingest(table_index, table, sender.clone()).await?;
        Ok(())
    }

    async fn snapshot(
        &self,
        table_index: usize,
        table: &TableInfo,
        sender: Sender<Result<Option<IngestionMessage>, ObjectStoreConnectorError>>,
    ) -> Result<
        JoinHandle<(usize, HashMap<object_store::path::Path, DateTime<Utc>>)>,
        ObjectStoreConnectorError,
    >;

    async fn ingest(
        &self,
        table_index: usize,
        table: &TableInfo,
        sender: Sender<Result<Option<IngestionMessage>, ObjectStoreConnectorError>>,
    ) -> Result<(), ObjectStoreConnectorError>;
}
