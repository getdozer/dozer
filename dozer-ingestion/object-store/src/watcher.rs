use std::path::Path;
use std::{collections::HashMap, sync::Arc, time::Duration};

use deltalake::{
    datafusion::{datasource::listing::ListingTableUrl, prelude::SessionContext},
    Path as DeltaPath,
};
use dozer_ingestion_connector::dozer_types::log::info;
use dozer_ingestion_connector::dozer_types::models::ingestion_types::IngestionMessage;
use dozer_ingestion_connector::futures::StreamExt;
use dozer_ingestion_connector::tokio::sync::mpsc::Sender;
use dozer_ingestion_connector::{async_trait, tokio, TableInfo};
use object_store::ObjectStore;

use crate::helper::map_listing_options;
use crate::{table_reader, ObjectStoreConnectorError, ObjectStoreObjectError};

use super::{adapters::DozerObjectStore, table_reader::TableReader};

const WATCHER_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Eq, Clone)]
struct FileInfo {
    _name: String,
    last_modified: i64,
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
        table_index: usize,
        table: &TableInfo,
        sender: Sender<Result<Option<IngestionMessage>, ObjectStoreConnectorError>>,
    ) -> Result<(), ObjectStoreConnectorError>;
}

#[async_trait]
impl<T: DozerObjectStore> Watcher<T> for TableReader<T> {
    async fn watch(
        &self,
        table_index: usize,
        table: &TableInfo,
        sender: Sender<Result<Option<IngestionMessage>, ObjectStoreConnectorError>>,
    ) -> Result<(), ObjectStoreConnectorError> {
        let params = self.config.table_params(&table.name)?;
        let store = Arc::new(params.object_store);

        let source_folder = params.folder.to_string();

        let mut source_state = HashMap::new();

        let base_path = params.table_path;

        let listing_options = map_listing_options(&params.data_fusion_table)
            .map_err(ObjectStoreConnectorError::DataFusionStorageObjectError)?;

        let ctx = SessionContext::new();

        ctx.runtime_env()
            .register_object_store(&params.url, store.clone());

        let t = table.clone();

        tokio::spawn(async move {
            loop {
                // List objects in the S3 bucket with the specified prefix
                let mut stream = store
                    .list(Some(&DeltaPath::from(source_folder.to_owned())))
                    .await
                    .unwrap();

                // Contains added files as FileInfo
                let mut new_files = vec![];

                while let Some(item) = stream.next().await {
                    // Check if any objects have been added or modified
                    let object = item.unwrap();

                    if let Some(last_modified) = source_state.get_mut(&object.location) {
                        // Scenario 1: Update on existing file
                        if *last_modified < object.last_modified {
                            info!(
                                "Source Object has been modified: {:?}, {:?}",
                                object.location, object.last_modified
                            );
                        }
                    } else {
                        // Scenario 2: New file added
                        info!(
                            "Source Object has been added: {:?}, {:?}",
                            object.location, object.last_modified
                        );

                        let file_path = object.location.to_string();
                        // Skip the source folder
                        if file_path == source_folder {
                            continue;
                        }

                        // Remove base folder from relative path
                        let path = Path::new(&file_path);
                        let new_path = path
                            .strip_prefix(path.components().next().unwrap())
                            .unwrap();
                        let new_path_str = new_path.to_str().unwrap();

                        new_files.push(FileInfo {
                            _name: base_path.clone() + new_path_str,
                            last_modified: object.last_modified.timestamp(),
                        });
                        source_state.insert(object.location, object.last_modified);
                    }
                }

                new_files.sort();
                for file in new_files {
                    let file_path = ListingTableUrl::parse(&file._name)
                        .map_err(|e| {
                            ObjectStoreConnectorError::DataFusionStorageObjectError(
                                ObjectStoreObjectError::ListingPathParsingError(
                                    file._name.clone(),
                                    e,
                                ),
                            )
                        })
                        .unwrap();

                    let result = table_reader::read(
                        table_index,
                        ctx.clone(),
                        file_path,
                        listing_options.clone(),
                        &t,
                        sender.clone(),
                    )
                    .await;
                    if let Err(e) = result {
                        sender.send(Err(e)).await.unwrap();
                    }
                }

                // Wait for 10 seconds before checking again
                tokio::time::sleep(WATCHER_INTERVAL).await;
            }
        });

        Ok(())
    }
}
