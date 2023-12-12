use std::{collections::HashMap, sync::Arc, time::Duration};

use deltalake::{
    datafusion::{common::DFSchema, datasource::listing::ListingTableUrl, prelude::SessionContext},
    Path,
};
use dozer_ingestion_connector::{
    dozer_types::{
        chrono::{DateTime, Utc},
        log::info,
        models::ingestion_types::{self, CsvConfig, IngestionMessage, ParquetConfig},
    },
    futures::StreamExt,
    tokio::{self, sync::mpsc::Sender},
    TableInfo,
};
use object_store::ObjectStore;

use crate::{
    adapters::DozerObjectStore,
    helper::{is_marker_file_exist, map_listing_options},
    table_reader,
    table_watcher::FileInfo,
    ObjectStoreConnectorError, ObjectStoreObjectError,
};

pub trait TableConfig {
    fn path(&self) -> &str;
    fn extension(&self) -> &str;
    fn marker_extension(&self) -> Option<&str>;
}

pub struct ObjectStoreTable<C: TableConfig, O: DozerObjectStore> {
    table_config: C,
    store: O,
    update_state: HashMap<Path, DateTime<Utc>>,
}

impl<C: TableConfig, O: DozerObjectStore> ObjectStoreTable<C, O> {
    pub fn new(table_config: C, store: O, update_state: HashMap<Path, DateTime<Utc>>) -> Self {
        Self {
            table_config,
            store,
            update_state,
        }
    }

    pub async fn snapshot(
        &self,
        table_index: usize,
        table_info: &TableInfo,
        sender: Sender<Result<Option<IngestionMessage>, ObjectStoreConnectorError>>,
    ) -> Result<(HashMap<Path, DateTime<Utc>>, Option<DFSchema>), ObjectStoreConnectorError> {
        let params = self.store.table_params(&table_info.name)?;
        let store = Arc::new(params.object_store);

        let listing_options = map_listing_options(&params.data_fusion_table)
            .map_err(ObjectStoreConnectorError::DataFusionStorageObjectError)?;

        let ctx = SessionContext::new();

        ctx.runtime_env()
            .register_object_store(&params.url, store.clone());

        // Get the table state after snapshot
        let mut update_state = self.update_state.clone();

        // List objects in the S3 bucket with the specified prefix
        let mut stream = store
            .list(Some(&Path::from(params.folder.clone())))
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
                // Scenario 2: New file added
                info!(
                    "Source Object has been added: {:?}, {:?}",
                    object.location, object.last_modified
                );

                let file_path = object.location.to_string();
                // Skip the source folder
                if file_path == params.folder {
                    continue;
                }

                if file_path.ends_with(self.table_config.extension()) {
                    // Scenario 2: New file added
                    info!(
                        "Source Object has been added: {:?}, {:?}",
                        object.location, object.last_modified
                    );

                    // Remove base folder from relative path
                    let path = std::path::Path::new(&file_path);
                    let new_path = path
                        .strip_prefix(path.components().next().unwrap())
                        .unwrap();
                    let new_path_str = new_path.to_str().unwrap();

                    new_files.push(FileInfo {
                        name: params.table_path.clone() + new_path_str,
                        last_modified: object.last_modified.timestamp(),
                    });
                    if self.table_config.marker_extension().is_none() {
                        update_state.insert(object.location, object.last_modified);
                    }
                } else if let Some(marker_extension) = self.table_config.marker_extension() {
                    if file_path.ends_with(marker_extension) {
                        // Scenario 3: New marker file added
                        info!(
                            "Source Object Marker has been added: {:?}, {:?}",
                            object.location, object.last_modified
                        );

                        // Remove base folder from relative path
                        let path = std::path::Path::new(&file_path);
                        let new_path = path
                            .strip_prefix(path.components().next().unwrap())
                            .unwrap();
                        let new_path_str = new_path.to_str().unwrap();

                        new_marker_files.push(FileInfo {
                            name: params.table_path.clone() + new_path_str,
                            last_modified: object.last_modified.timestamp(),
                        });

                        update_state.insert(object.location, object.last_modified);
                    } else {
                        continue;
                    }
                } else {
                    // Skip files that do not match the extension nor marker extension
                    continue;
                }
            }
        }

        let mut schema = None;
        new_files.sort();
        for file in &new_files {
            let marker_file_exist = is_marker_file_exist(new_marker_files.clone(), file);
            if !marker_file_exist && self.table_config.marker_extension().is_some() {
                continue;
            } else {
                let file_path = ListingTableUrl::parse(&file.name)
                    .map_err(|e| {
                        ObjectStoreConnectorError::DataFusionStorageObjectError(
                            ObjectStoreObjectError::ListingPathParsingError(file.name.clone(), e),
                        )
                    })
                    .unwrap();

                let result = table_reader::read(
                    table_index,
                    ctx.clone(),
                    file_path,
                    listing_options.clone(),
                    table_info,
                    sender.clone(),
                    schema.as_ref(),
                )
                .await;
                match result {
                    Ok(s) => {
                        schema = Some(s);
                    }
                    Err(e) => sender.send(Err(e)).await.unwrap(),
                }
            }
        }

        Ok((update_state, schema))
    }

    pub async fn watch(
        &self,
        table_index: usize,
        table_info: &TableInfo,
        sender: Sender<Result<Option<IngestionMessage>, ObjectStoreConnectorError>>,
        schema: Option<&DFSchema>,
    ) -> Result<(), ObjectStoreConnectorError> {
        let params = self.store.table_params(&table_info.name)?;
        let store = Arc::new(params.object_store);

        let source_folder = params.folder.to_string();
        let base_path = params.table_path;

        let listing_options = map_listing_options(&params.data_fusion_table)
            .map_err(ObjectStoreConnectorError::DataFusionStorageObjectError)?;

        let ctx = SessionContext::new();

        ctx.runtime_env()
            .register_object_store(&params.url, store.clone());

        // Get the table state after snapshot
        let mut update_state = self.update_state.clone();

        loop {
            // List objects in the S3 bucket with the specified prefix
            let mut stream = store
                .list(Some(&Path::from(source_folder.to_owned())))
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

                    if file_path.ends_with(self.table_config.extension()) {
                        // Scenario 2: New file added
                        info!(
                            "Source Object has been added: {:?}, {:?}",
                            object.location, object.last_modified
                        );

                        // Remove base folder from relative path
                        let path = std::path::Path::new(&file_path);
                        let new_path = path
                            .strip_prefix(path.components().next().unwrap())
                            .unwrap();
                        let new_path_str = new_path.to_str().unwrap();

                        new_files.push(FileInfo {
                            name: base_path.clone() + new_path_str,
                            last_modified: object.last_modified.timestamp(),
                        });
                        if self.table_config.marker_extension().is_none() {
                            update_state.insert(object.location, object.last_modified);
                        }
                    } else if let Some(marker_extension) = self.table_config.marker_extension() {
                        if file_path.ends_with(marker_extension) {
                            // Scenario 3: New marker file added
                            info!(
                                "Source Object Marker has been added: {:?}, {:?}",
                                object.location, object.last_modified
                            );

                            // Remove base folder from relative path
                            let path = std::path::Path::new(&file_path);
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
                            continue;
                        }
                    } else {
                        // Skip files that do not match the extension nor marker extension
                        continue;
                    }
                }
            }

            new_files.sort();
            for file in &new_files {
                let marker_file_exist = is_marker_file_exist(new_marker_files.clone(), file);
                if !marker_file_exist && self.table_config.marker_extension().is_some() {
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

                    let result = table_reader::read(
                        table_index,
                        ctx.clone(),
                        file_path,
                        listing_options.clone(),
                        table_info,
                        sender.clone(),
                        schema,
                    )
                    .await;
                    if let Err(e) = result {
                        sender.send(Err(e)).await.unwrap();
                    }
                }
            }

            // Wait for 10 seconds before checking again
            const WATCHER_INTERVAL: Duration = Duration::from_secs(1);
            tokio::time::sleep(WATCHER_INTERVAL).await;
        }
    }
}

impl TableConfig for CsvConfig {
    fn path(&self) -> &str {
        &self.path
    }

    fn extension(&self) -> &str {
        &self.extension
    }

    fn marker_extension(&self) -> Option<&str> {
        self.marker_extension.as_deref()
    }
}

impl TableConfig for ParquetConfig {
    fn path(&self) -> &str {
        &self.path
    }

    fn extension(&self) -> &str {
        &self.extension
    }

    fn marker_extension(&self) -> Option<&str> {
        self.marker_extension.as_deref()
    }
}

impl TableConfig for ingestion_types::TableConfig {
    fn path(&self) -> &str {
        match self {
            ingestion_types::TableConfig::CSV(csv_config) => csv_config.path(),
            ingestion_types::TableConfig::Parquet(parquet_config) => parquet_config.path(),
        }
    }

    fn extension(&self) -> &str {
        match self {
            ingestion_types::TableConfig::CSV(csv_config) => csv_config.extension(),
            ingestion_types::TableConfig::Parquet(parquet_config) => parquet_config.extension(),
        }
    }

    fn marker_extension(&self) -> Option<&str> {
        match self {
            ingestion_types::TableConfig::CSV(csv_config) => csv_config.marker_extension(),
            ingestion_types::TableConfig::Parquet(parquet_config) => {
                parquet_config.marker_extension()
            }
        }
    }
}
