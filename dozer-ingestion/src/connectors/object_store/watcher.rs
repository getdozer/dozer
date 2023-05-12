use std::{sync::Arc, time::Duration};

use deltalake::{datafusion::datasource::listing::ListingTableUrl, Path};
use dozer_types::types::Operation;
use futures::StreamExt;
use object_store::ObjectStore;
use tokio::sync::mpsc::Sender;
use tonic::async_trait;

use crate::{
    connectors::TableInfo,
    errors::{ConnectorError, ObjectStoreConnectorError, ObjectStoreObjectError},
};

use super::{adapters::DozerObjectStore, table_reader::TableReader};

const WATCHER_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Eq, Clone)]
struct FileInfo {
    name: String,
    last_modified: u64,
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
pub trait Watcher<T> {
    async fn watch(
        &self,
        id: u32,
        table: &TableInfo,
        sender: Sender<Result<Option<Operation>, ObjectStoreConnectorError>>,
    ) -> Result<(), ConnectorError>;
}

#[async_trait]
impl<T: DozerObjectStore> Watcher<T> for TableReader<T> {
    async fn watch(
        &self,
        id: u32,
        table: &TableInfo,
        sender: Sender<Result<Option<Operation>, ObjectStoreConnectorError>>,
    ) -> Result<(), ConnectorError> {
        let params = self.config.table_params(&table.name)?;
        let store = Arc::new(params.object_store);

        tokio::spawn(async move {
            loop {
                // List objects in the S3 bucket with the specified prefix
                let mut stream = store.list(Some(&Path::from(params.folder))).await.unwrap();

                while let Some(item) = stream.next().await {
                    // Check if any objects have been added or modified
                    let object = item.unwrap();
                    println!(
                        "Found object: {:?}, {:?}",
                        object.location, object.last_modified
                    );

                    // Do something with the object, such as download it or process it
                }

                let table_path = ListingTableUrl::parse("data/trips")
                    .map_err(|e| {
                        ObjectStoreConnectorError::DataFusionStorageObjectError(
                            ObjectStoreObjectError::ListingPathParsingError(
                                params.table_path.clone(),
                                e,
                            ),
                        )
                    })
                    .unwrap();

                // Wait for 10 seconds before checking again
                tokio::time::sleep(WATCHER_INTERVAL).await;
            }
        });

        Ok(())
    }
}
