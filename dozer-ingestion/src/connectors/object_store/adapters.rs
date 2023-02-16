use crate::connectors::object_store::helper::{get_details, get_table};
use crate::errors::ConnectorError;
use dozer_types::ingestion_types::{LocalStorage, S3Storage, Table};
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;

pub trait DozerObjectStore: Clone + Send + Sync {
    type ObjectStore: ObjectStore;

    fn table_params<'a>(
        &'a self,
        table_name: &str,
    ) -> Result<DozerObjectStoreParams<'a, Self::ObjectStore>, ConnectorError>;

    fn tables(&self) -> &[Table];
}

pub struct DozerObjectStoreParams<'a, T: ObjectStore> {
    pub scheme: &'static str,
    pub host: &'a str,
    pub object_store: T,
    pub table_path: String,
    pub data_fusion_table: &'a Table,
}

impl DozerObjectStore for S3Storage {
    type ObjectStore = AmazonS3;

    fn table_params(
        &self,
        table_name: &str,
    ) -> Result<DozerObjectStoreParams<Self::ObjectStore>, ConnectorError> {
        let table = get_table(&self.tables, table_name)?;
        let details = get_details(&self.details)?;

        let object_store = AmazonS3Builder::new()
            .with_bucket_name(&details.bucket_name)
            .with_region(&details.region)
            .with_access_key_id(&details.access_key_id)
            .with_secret_access_key(&details.secret_access_key)
            .build()
            .map_err(|_| ConnectorError::InitializationError)?;

        Ok(DozerObjectStoreParams {
            scheme: "s3",
            host: &details.bucket_name,
            object_store,
            table_path: format!("s3://{}/{}/", details.bucket_name, table.prefix),
            data_fusion_table: table,
        })
    }

    fn tables(&self) -> &[Table] {
        &self.tables
    }
}

impl DozerObjectStore for LocalStorage {
    type ObjectStore = LocalFileSystem;

    fn table_params(
        &self,
        table_name: &str,
    ) -> Result<DozerObjectStoreParams<Self::ObjectStore>, ConnectorError> {
        let table = get_table(&self.tables, table_name)?;
        let path = get_details(&self.details)?.path.as_str();

        let object_store = LocalFileSystem::new_with_prefix(path)
            .map_err(|_| ConnectorError::InitializationError)?;

        Ok(DozerObjectStoreParams {
            scheme: "local",
            host: path,
            object_store,
            table_path: format!("s3://{path}/{}/", table.prefix),
            data_fusion_table: table,
        })
    }

    fn tables(&self) -> &[Table] {
        &self.tables
    }
}
