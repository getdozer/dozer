use crate::errors::ObjectStoreObjectError::{MissingStorageDetails, TableDefinitionNotFound};
use crate::errors::{ConnectorError, ObjectStoreConnectorError};
use dozer_types::ingestion_types::{LocalStorage, S3Storage, Table};
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use std::fmt::Debug;

pub trait DozerObjectStore: Clone + Send + Sync + Debug {
    type ObjectStore: ObjectStore;

    fn table_params(
        &self,
        table_name: &str,
    ) -> Result<DozerObjectStoreParams<Self::ObjectStore>, ConnectorError> {
        let table = self
            .tables()
            .iter()
            .find(|table| table.name == table_name)
            .ok_or(ObjectStoreConnectorError::DataFusionStorageObjectError(
                TableDefinitionNotFound,
            ))?;

        self.store_params(table)
    }

    fn store_params(
        &self,
        table: &Table,
    ) -> Result<DozerObjectStoreParams<Self::ObjectStore>, ConnectorError>;

    fn tables(&self) -> &[Table];
}

pub struct DozerObjectStoreParams<T: ObjectStore> {
    pub scheme: String,
    pub host: String,
    pub object_store: T,

    pub table_path: String,
    pub folder: String,
    pub data_fusion_table: Table,

    // todo: refactor this datastructure
    pub aws_region: Option<String>,
    pub aws_access_key_id: Option<String>,
    pub aws_secret_access_key: Option<String>,
}

impl DozerObjectStore for S3Storage {
    type ObjectStore = AmazonS3;

    fn store_params(
        &self,
        table: &Table,
    ) -> Result<DozerObjectStoreParams<Self::ObjectStore>, ConnectorError> {
        let details = get_details(&self.details)?;

        let object_store = AmazonS3Builder::new()
            .with_bucket_name(&details.bucket_name)
            .with_region(&details.region)
            .with_access_key_id(&details.access_key_id)
            .with_secret_access_key(&details.secret_access_key)
            .build()
            .map_err(|e| ConnectorError::InitializationError(e.to_string()))?;

        if table.config.is_none() {
            return Err(ConnectorError::TableNotFound(format!("{} - Table configuration for Parquet and CSV is changed since v0.1.27, please check our documentation at https://getdozer.io/docs/configuration/connectors/#source-table-types-for-connectors", table.name.clone())));
        }

        let folder = if let Some(config) = &table.config {
            match config {
                dozer_types::ingestion_types::TableConfig::CSV(csv_config) => {
                    csv_config.path.clone()
                }
                dozer_types::ingestion_types::TableConfig::Delta(delta_config) => {
                    delta_config.path.clone()
                }
                dozer_types::ingestion_types::TableConfig::Parquet(parquet_config) => {
                    parquet_config.path.clone()
                }
            }
        } else {
            return Err(ConnectorError::TableNotFound(table.name.clone()));
        };

        Ok(DozerObjectStoreParams {
            scheme: "s3".to_string(),
            host: details.bucket_name.clone(),
            object_store,
            table_path: format!("s3://{}/{folder}/", details.bucket_name),
            folder,
            data_fusion_table: table.clone(),

            aws_region: Some(details.region.clone()),
            aws_access_key_id: Some(details.access_key_id.clone()),
            aws_secret_access_key: Some(details.secret_access_key.clone()),
        })
    }

    fn tables(&self) -> &[Table] {
        &self.tables
    }
}

impl DozerObjectStore for LocalStorage {
    type ObjectStore = LocalFileSystem;

    fn store_params(
        &self,
        table: &Table,
    ) -> Result<DozerObjectStoreParams<Self::ObjectStore>, ConnectorError> {
        let path = get_details(&self.details)?.path.as_str();

        let object_store = LocalFileSystem::new_with_prefix(path)
            .map_err(|e| ConnectorError::InitializationError(e.to_string()))?;

        if table.config.is_none() {
            return Err(ConnectorError::TableNotFound(format!("`{}` - Table configuration for Parquet and CSV is changed since v0.1.27, please check our documentation at https://getdozer.io/docs/configuration/connectors/#source-table-types-for-connectors", table.name.clone())));
        }

        let folder = if let Some(config) = &table.config {
            match config {
                dozer_types::ingestion_types::TableConfig::CSV(csv_config) => {
                    csv_config.path.clone()
                }
                dozer_types::ingestion_types::TableConfig::Delta(delta_config) => {
                    delta_config.path.clone()
                }
                dozer_types::ingestion_types::TableConfig::Parquet(parquet_config) => {
                    parquet_config.path.clone()
                }
            }
        } else {
            return Err(ConnectorError::TableNotFound(table.name.clone()));
        };

        Ok(DozerObjectStoreParams {
            scheme: "local".to_string(),
            host: path.to_owned(),
            object_store,
            table_path: format!("{path}/{folder}/"),
            folder,
            data_fusion_table: table.clone(),

            aws_region: None,
            aws_access_key_id: None,
            aws_secret_access_key: None,
        })
    }

    fn tables(&self) -> &[Table] {
        &self.tables
    }
}

fn get_details<T>(details: &Option<T>) -> Result<&T, ObjectStoreConnectorError> {
    details
        .as_ref()
        .ok_or(ObjectStoreConnectorError::DataFusionStorageObjectError(
            MissingStorageDetails,
        ))
}
