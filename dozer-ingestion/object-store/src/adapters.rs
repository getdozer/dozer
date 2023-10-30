use dozer_ingestion_connector::dozer_types::models::ingestion_types::{
    LocalStorage, S3Storage, Table, TableConfig,
};
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::local::LocalFileSystem;
use object_store::{BackoffConfig, ObjectStore, RetryConfig};
use std::fmt::Debug;
use url::Url;

use crate::{ObjectStoreConnectorError, ObjectStoreObjectError};

pub trait DozerObjectStore: Clone + Send + Sync + Debug + 'static {
    type ObjectStore: ObjectStore;

    fn table_params(
        &self,
        table_name: &str,
    ) -> Result<DozerObjectStoreParams<Self::ObjectStore>, ObjectStoreConnectorError> {
        let table = self
            .tables()
            .iter()
            .find(|table| table.name == table_name)
            .ok_or(ObjectStoreConnectorError::DataFusionStorageObjectError(
                ObjectStoreObjectError::TableDefinitionNotFound,
            ))?;

        self.store_params(table)
    }

    fn store_params(
        &self,
        table: &Table,
    ) -> Result<DozerObjectStoreParams<Self::ObjectStore>, ObjectStoreConnectorError>;

    fn tables(&self) -> &[Table];
}

pub struct DozerObjectStoreParams<T: ObjectStore> {
    pub url: Url,
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
    ) -> Result<DozerObjectStoreParams<Self::ObjectStore>, ObjectStoreConnectorError> {
        let details = &self.details;

        let retry_config = RetryConfig {
            backoff: BackoffConfig::default(),
            max_retries: usize::max_value(),
            retry_timeout: std::time::Duration::from_secs(u64::MAX),
        };

        let object_store = AmazonS3Builder::new()
            .with_bucket_name(&details.bucket_name)
            .with_region(&details.region)
            .with_access_key_id(&details.access_key_id)
            .with_secret_access_key(&details.secret_access_key)
            .with_retry(retry_config)
            .build()?;

        let folder = match &table.config {
            TableConfig::CSV(csv_config) => csv_config.path.clone(),
            TableConfig::Parquet(parquet_config) => parquet_config.path.clone(),
        };

        Ok(DozerObjectStoreParams {
            url: Url::parse(&format!("s3://{}", details.bucket_name)).expect("Must be valid url"),
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
    ) -> Result<DozerObjectStoreParams<Self::ObjectStore>, ObjectStoreConnectorError> {
        let path = &self.details.path.as_str();

        let object_store = LocalFileSystem::new_with_prefix(path)?;

        let folder = match &table.config {
            TableConfig::CSV(csv_config) => csv_config.path.clone(),
            TableConfig::Parquet(parquet_config) => parquet_config.path.clone(),
        };

        Ok(DozerObjectStoreParams {
            url: Url::parse(&format!("local://{}", path)).expect("Must be valid url"),
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
