use crate::{
    connectors::{
        self,
        object_store::{helper::map_listing_options, table_watcher::FileInfo},
    },
    errors::ObjectStoreObjectError,
};
use deltalake::{
    datafusion::{datasource::listing::ListingTableUrl, prelude::SessionContext},
    Path as DeltaPath,
};

use dozer_types::{
    chrono::{DateTime, Utc},
    ingestion_types::ParquetConfig,
    tracing::info,
    types::Operation,
};
use futures::StreamExt;
use object_store::ObjectStore;
use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};
use tokio::sync::mpsc::Sender;
use tonic::async_trait;

use crate::{
    connectors::{
        object_store::{adapters::DozerObjectStore, table_watcher::TableWatcher},
        TableInfo,
    },
    errors::{ConnectorError, ObjectStoreConnectorError},
};

const WATCHER_INTERVAL: Duration = Duration::from_secs(1);

pub struct ParquetTable<T: DozerObjectStore + Send> {
    table_config: ParquetConfig,
    store_config: T,
    update_state: HashMap<DeltaPath, DateTime<Utc>>,
}

impl<T: DozerObjectStore + Send> ParquetTable<T> {
    pub fn new(table_config: ParquetConfig, store_config: T) -> Self {
        Self {
            table_config,
            store_config,
            update_state: HashMap::new(),
        }
    }

    async fn read(
        &self,
        id: u32,
        table: &TableInfo,
        sender: Sender<Result<Option<Operation>, ObjectStoreConnectorError>>,
    ) -> Result<(), ConnectorError> {
        let params = self.store_config.table_params(&table.name)?;
        let store = Arc::new(params.object_store);

        let source_folder = params.folder.to_string();
        let base_path = params.table_path;

        let listing_options = map_listing_options(&params.data_fusion_table)
            .map_err(ObjectStoreConnectorError::DataFusionStorageObjectError)?;

        let ctx = SessionContext::new();

        ctx.runtime_env()
            .register_object_store(params.scheme, params.host, store.clone());

        let t = table.clone();

        // Get the table state after snapshot
        let mut update_state = self.update_state.clone();

        let extension = self.table_config.extension.clone();

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

                    if let Some(last_modified) = update_state.get_mut(&object.location) {
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

                        // Skip files that do not match the extension
                        if !file_path.ends_with(extension.as_str()) {
                            continue;
                        }

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
                        update_state.insert(object.location, object.last_modified);
                    }
                }

                new_files.sort();
                for file in new_files {
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
                        id,
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

#[async_trait]
impl<T: DozerObjectStore + Send> TableWatcher for ParquetTable<T> {
    async fn snapshot(
        &self,
        id: usize,
        table: &TableInfo,
        sender: Sender<Result<Option<Operation>, ObjectStoreConnectorError>>,
    ) -> Result<(), ConnectorError> {
        let params = self.store_config.table_params(&table.name)?;
        let store = Arc::new(params.object_store);

        let source_folder = params.folder.to_string();
        let base_path = params.table_path;

        let listing_options = map_listing_options(&params.data_fusion_table)
            .map_err(ObjectStoreConnectorError::DataFusionStorageObjectError)?;

        let ctx = SessionContext::new();

        ctx.runtime_env()
            .register_object_store(params.scheme, params.host, store.clone());

        let t = table.clone();

        // Get the table state after snapshot
        let mut update_state = self.update_state.clone();

        let extension = self.table_config.extension.clone();

        tokio::spawn(async move {
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

                if let Some(last_modified) = update_state.get_mut(&object.location) {
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

                    // Skip files that do not match the extension
                    if !file_path.ends_with(extension.as_str()) {
                        continue;
                    }

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
                    update_state.insert(object.location, object.last_modified);
                }
            }

            new_files.sort();
            for file in new_files {
                let file_path = ListingTableUrl::parse(&file.name)
                    .map_err(|e| {
                        ObjectStoreConnectorError::DataFusionStorageObjectError(
                            ObjectStoreObjectError::ListingPathParsingError(file.name.clone(), e),
                        )
                    })
                    .unwrap();

                let result = connectors::object_store::table_reader::TableReader::<T>::read(
                    id as u32,
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
        });

        Ok(())
    }

    async fn ingest(
        &self,
        _id: usize,
        _table: &TableInfo,
        _sender: Sender<Result<Option<Operation>, ObjectStoreConnectorError>>,
    ) -> Result<(), ConnectorError> {
        // self.read(id as u32, table, sender).await?;
        Ok(())
    }
}
