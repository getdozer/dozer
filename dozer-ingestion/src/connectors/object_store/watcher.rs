use std::{collections::HashMap, sync::Arc, time::Duration};

use deltalake::{
    datafusion::{datasource::listing::ListingTableUrl, prelude::SessionContext},
    Path,
};
use dozer_types::{tracing::info, types::Operation};
use futures::StreamExt;
use object_store::ObjectStore;
use tokio::sync::mpsc::Sender;
use tonic::async_trait;

use crate::{
    connectors::{object_store::helper::map_listing_options, TableInfo},
    errors::{ConnectorError, ObjectStoreConnectorError, ObjectStoreObjectError},
};

use super::{adapters::DozerObjectStore, table_reader::TableReader};

const WATCHER_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Eq, Clone)]
struct FileInfo {
    _name: String,
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

        let source_folder = params.folder.to_string();

        let mut source_state = HashMap::new();

        let table_path = ListingTableUrl::parse(&params.table_path).map_err(|e| {
            ObjectStoreConnectorError::DataFusionStorageObjectError(
                ObjectStoreObjectError::ListingPathParsingError(params.table_path.clone(), e),
            )
        })?;
        let listing_options = map_listing_options(params.data_fusion_table)
            .map_err(ObjectStoreConnectorError::DataFusionStorageObjectError)?;

        let ctx = SessionContext::new();

        ctx.runtime_env()
            .register_object_store(params.scheme, params.host, store.clone());

        let t = table.clone();

        tokio::spawn(async move {
            loop {
                // List objects in the S3 bucket with the specified prefix
                let mut stream = store
                    .list(Some(&Path::from(source_folder.to_owned())))
                    .await
                    .unwrap();

                while let Some(item) = stream.next().await {
                    // Check if any objects have been added or modified
                    let object = item.unwrap();
                    info!(
                        "Source object: {:?}, {:?}",
                        object.location, object.last_modified
                    );

                    if let Some(last_modified) = source_state.get_mut(&object.location) {
                        if *last_modified < object.last_modified {
                            info!("Source Object has been modified");
                        }
                    } else {
                        println!("Source Object has been added");
                        source_state.insert(object.location, object.last_modified);

                        let result = Self::read(
                            id,
                            ctx.clone(),
                            table_path.clone(),
                            listing_options.clone(),
                            &t,
                            sender.clone(),
                        )
                        .await;
                        if let Err(e) = result {
                            sender.send(Err(e)).await.unwrap();
                        }
                    }
                }

                // Wait for 10 seconds before checking again
                tokio::time::sleep(WATCHER_INTERVAL).await;
            }
        });

        Ok(())
    }
}
