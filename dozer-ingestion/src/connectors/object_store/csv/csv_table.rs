use dozer_types::{
    chrono::{DateTime, Utc},
    ingestion_types::{CsvConfig, IngestionMessage},
    tracing::info,
};
use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};

use futures::StreamExt;
use object_store::ObjectStore;
use tokio::sync::mpsc::Sender;
use tonic::async_trait;

use crate::{
    connectors::{
        object_store::{
            adapters::DozerObjectStore,
            table_watcher::{FileInfo, TableWatcher},
        },
        TableInfo,
    },
    errors::{ConnectorError, ObjectStoreConnectorError},
};

use deltalake::{
    datafusion::{datasource::listing::ListingTableUrl, prelude::SessionContext},
    Path as DeltaPath,
};
use tokio::task::JoinHandle;

use crate::connectors::object_store::helper::is_marker_file_exist;
use crate::{
    connectors::{self, object_store::helper::map_listing_options},
    errors::ObjectStoreObjectError,
};

const _WATCHER_INTERVAL: Duration = Duration::from_secs(1);

pub struct CsvTable<T: DozerObjectStore + Send> {
    table_config: CsvConfig,
    store_config: T,
    pub update_state: HashMap<DeltaPath, DateTime<Utc>>,
}

impl<T: DozerObjectStore + Send> CsvTable<T> {
    pub fn new(table_config: CsvConfig, store_config: T) -> Self {
        Self {
            table_config,
            store_config,
            update_state: HashMap::new(),
        }
    }

    async fn read(
        &self,
        table_index: usize,
        table: &TableInfo,
        sender: Sender<Result<Option<IngestionMessage>, ObjectStoreConnectorError>>,
    ) -> Result<(), ConnectorError> {
        let params = self.store_config.table_params(&table.name)?;
        let store = Arc::new(params.object_store);

        let source_folder = params.folder.to_string();
        let base_path = params.table_path;

        let listing_options = map_listing_options(&params.data_fusion_table)
            .map_err(ObjectStoreConnectorError::DataFusionStorageObjectError)?;

        let ctx = SessionContext::new();

        ctx.runtime_env()
            .register_object_store(&params.url, store.clone());

        let t = table.clone();

        // Get the table state after snapshot
        let mut update_state = self.update_state.clone();
        let extension = self.table_config.extension.clone();
        let marker_extension = match self.table_config.marker_file {
            true => self.table_config.marker_extension.clone(),
            false => String::new(),
        };

        loop {
            // List objects in the S3 bucket with the specified prefix
            let mut stream = store
                .list(Some(&DeltaPath::from(source_folder.to_owned())))
                .await
                .unwrap();

            let mut new_files = vec![];
            let mut new_marker_files = vec![];

            while let Some(item) = stream.next().await {
                // Check if any objects have been added or modified
                let object = item.unwrap();

                if let Some(last_modified) = update_state.get_mut(&object.location) {
                    // Scenario 1: Update on existing file
                    if *last_modified < object.last_modified {
                        info!(
                            "Source Object has been modified: {:?}, {:?}",
                            object.location, object.last_modified
                        );
                    }
                } else {
                    let file_path = object.location.to_string();
                    // Skip the source folder
                    if file_path == source_folder {
                        continue;
                    }

                    if file_path.ends_with(extension.as_str()) {
                        // Scenario 2: New file added
                        info!(
                            "Source Object has been added: {:?}, {:?}",
                            object.location, object.last_modified
                        );

                        // Remove base folder from relative path
                        let path = Path::new(&file_path);
                        let new_path = path
                            .strip_prefix(path.components().next().unwrap())
                            .unwrap();
                        let new_path_str = new_path.to_str().unwrap();

                        new_files.push(FileInfo {
                            name: base_path.clone() + new_path_str,
                            last_modified: object.last_modified.timestamp(),
                        });
                        if marker_extension.is_empty() {
                            update_state.insert(object.location, object.last_modified);
                        }
                    } else if file_path.ends_with(marker_extension.as_str())
                        && !marker_extension.is_empty()
                    {
                        // Scenario 3: New marker file added
                        info!(
                            "Source Object Marker has been added: {:?}, {:?}",
                            object.location, object.last_modified
                        );

                        // Remove base folder from relative path
                        let path = Path::new(&file_path);
                        let new_path = path
                            .strip_prefix(path.components().next().unwrap())
                            .unwrap();
                        let new_path_str = new_path.to_str().unwrap();

                        new_marker_files.push(FileInfo {
                            name: base_path.clone() + new_path_str,
                            last_modified: object.last_modified.timestamp(),
                        });
                        update_state.insert(object.location, object.last_modified);
                    } else {
                        // Skip files that do not match the extension nor marker extension
                        continue;
                    }
                }
            }

            new_files.sort();
            for file in &new_files {
                let marker_file_exist = is_marker_file_exist(new_marker_files.clone(), file);
                let use_marker_file = marker_extension.is_empty();
                if !marker_file_exist && !use_marker_file {
                    continue;
                } else {
                    let file_path = ListingTableUrl::parse(&file.name)
                        .map_err(|e| {
                            ObjectStoreConnectorError::DataFusionStorageObjectError(
                                ObjectStoreObjectError::ListingPathParsingError(
                                    file.name.clone(),
                                    e,
                                ),
                            )
                        })
                        .unwrap();

                    let result = connectors::object_store::table_reader::TableReader::<T>::read(
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
            }

            // Wait for 10 seconds before checking again
            tokio::time::sleep(_WATCHER_INTERVAL).await;
        }
    }
}

#[async_trait]
impl<T: DozerObjectStore + Send> TableWatcher for CsvTable<T> {
    async fn snapshot(
        &self,
        table_index: usize,
        table: &TableInfo,
        sender: Sender<Result<Option<IngestionMessage>, ObjectStoreConnectorError>>,
    ) -> Result<JoinHandle<(usize, HashMap<object_store::path::Path, DateTime<Utc>>)>, ConnectorError>
    {
        let params = self.store_config.table_params(&table.name)?;
        let store = Arc::new(params.object_store);

        let source_folder = params.folder.to_string();
        let base_path = params.table_path;

        let listing_options = map_listing_options(&params.data_fusion_table)
            .map_err(ObjectStoreConnectorError::DataFusionStorageObjectError)?;

        let ctx = SessionContext::new();

        ctx.runtime_env()
            .register_object_store(&params.url, store.clone());

        let t = table.clone();

        // Get the table state after snapshot
        let mut update_state = self.update_state.clone();
        let extension = self.table_config.extension.clone();
        let marker_extension = match self.table_config.marker_file {
            true => self.table_config.marker_extension.clone(),
            false => String::new(),
        };

        let h = tokio::spawn(async move {
            // List objects in the S3 bucket with the specified prefix
            let mut stream = store
                .list(Some(&DeltaPath::from(source_folder.to_owned())))
                .await
                .unwrap();

            let mut new_files = vec![];
            let mut new_marker_files = vec![];

            while let Some(item) = stream.next().await {
                // Check if any objects have been added or modified
                let object = item.unwrap();

                if let Some(last_modified) = update_state.get_mut(&object.location) {
                    // Scenario 1: Update on existing file
                    if *last_modified < object.last_modified {
                        info!(
                            "Source Object has been modified: {:?}, {:?}",
                            object.location, object.last_modified
                        );
                    }
                } else {
                    let file_path = object.location.to_string();
                    // Skip the source folder
                    if file_path == source_folder {
                        continue;
                    }

                    if file_path.ends_with(extension.as_str()) {
                        // Scenario 2: New file added
                        info!(
                            "Source Object has been added: {:?}, {:?}",
                            object.location, object.last_modified
                        );

                        // Remove base folder from relative path
                        let path = Path::new(&file_path);
                        let new_path = path
                            .strip_prefix(path.components().next().unwrap())
                            .unwrap();
                        let new_path_str = new_path.to_str().unwrap();

                        new_files.push(FileInfo {
                            name: base_path.clone() + new_path_str,
                            last_modified: object.last_modified.timestamp(),
                        });
                        if marker_extension.is_empty() {
                            update_state.insert(object.location, object.last_modified);
                        }
                    } else if file_path.ends_with(marker_extension.as_str())
                        && !marker_extension.is_empty()
                    {
                        // Scenario 3: New marker file added
                        info!(
                            "Source Object Marker has been added: {:?}, {:?}",
                            object.location, object.last_modified
                        );

                        // Remove base folder from relative path
                        let path = Path::new(&file_path);
                        let new_path = path
                            .strip_prefix(path.components().next().unwrap())
                            .unwrap();
                        let new_path_str = new_path.to_str().unwrap();

                        new_marker_files.push(FileInfo {
                            name: base_path.clone() + new_path_str,
                            last_modified: object.last_modified.timestamp(),
                        });

                        update_state.insert(object.location, object.last_modified);
                    } else {
                        // Skip files that do not match the extension nor marker extension
                        continue;
                    }
                }
            }

            new_files.sort();
            for file in &new_files {
                let marker_file_exist = is_marker_file_exist(new_marker_files.clone(), file);
                let use_marker_file = marker_extension.is_empty();
                if !marker_file_exist && !use_marker_file {
                    continue;
                } else {
                    let file_path = ListingTableUrl::parse(&file.name)
                        .map_err(|e| {
                            ObjectStoreConnectorError::DataFusionStorageObjectError(
                                ObjectStoreObjectError::ListingPathParsingError(
                                    file.name.clone(),
                                    e,
                                ),
                            )
                        })
                        .unwrap();

                    let result = connectors::object_store::table_reader::TableReader::<T>::read(
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
            }
            (table_index, update_state)
        });

        Ok(h)
    }

    async fn ingest(
        &self,
        table_index: usize,
        table: &TableInfo,
        sender: Sender<Result<Option<IngestionMessage>, ObjectStoreConnectorError>>,
    ) -> Result<(), ConnectorError> {
        self.read(table_index, table, sender).await?;
        Ok(())
    }
}
