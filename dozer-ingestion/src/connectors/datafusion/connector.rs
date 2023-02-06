use std::sync::Arc;

use crate::connectors::TableInfo;
use crate::errors::ConnectorError;
use crate::{connectors::Connector, ingestion::Ingestor};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use dozer_types::ingestion_types::DataFusionConfig;
use dozer_types::parking_lot::RwLock;
use object_store::aws::AmazonS3Builder;

pub struct DataFusionConnector {
    pub id: u64,
    config: DataFusionConfig,
    ingestor: Option<Arc<RwLock<Ingestor>>>,
    tables: Option<Vec<TableInfo>>,
}

impl DataFusionConnector {
    pub fn new(id: u64, config: DataFusionConfig) -> Self {
        Self {
            id,
            config,
            ingestor: None,
            tables: None,
        }
    }
}

impl Connector for DataFusionConnector {
    fn get_schemas(
        &self,
        _table_names: Option<Vec<crate::connectors::TableInfo>>,
    ) -> Result<Vec<dozer_types::types::SchemaWithChangesType>, crate::errors::ConnectorError> {
        todo!()
    }

    fn get_tables(
        &self,
    ) -> Result<Vec<crate::connectors::TableInfo>, crate::errors::ConnectorError> {
        todo!()
    }

    fn test_connection(&self) -> Result<(), crate::errors::ConnectorError> {
        todo!()
    }

    fn initialize(
        &mut self,
        ingestor: Arc<RwLock<Ingestor>>,
        tables: Option<Vec<TableInfo>>,
    ) -> Result<(), ConnectorError> {
        self.ingestor = Some(ingestor);
        self.tables = tables;
        Ok(())
    }

    fn start(&self, _from_seq: Option<(u64, u64)>) -> Result<(), crate::errors::ConnectorError> {
        let ctx = SessionContext::new();

        let s3 = AmazonS3Builder::new()
            .with_bucket_name(self.config.bucket_name.to_owned())
            .with_region(self.config.region.to_owned())
            .with_access_key_id(self.config.access_key_id.to_owned())
            .with_secret_access_key(self.config.secret_access_key.to_owned())
            .build()
            .map_or(Err(ConnectorError::InitializationError), Ok)?;

        ctx.runtime_env()
            .register_object_store("s3", &self.config.bucket_name, Arc::new(s3));

        let _path = format!("s3://{}{}", self.config.bucket_name, self.config.path);
        // ctx.register_parquet("trips", &path, ParquetReadOptions::default())
        //     .await?;

        Ok(())
    }

    fn stop(&self) {
        todo!()
    }

    fn validate(
        &self,
        _tables: Option<Vec<crate::connectors::TableInfo>>,
    ) -> Result<(), crate::errors::ConnectorError> {
        todo!()
    }

    fn validate_schemas(
        &self,
        _tables: &[crate::connectors::TableInfo],
    ) -> Result<crate::connectors::ValidationResults, crate::errors::ConnectorError> {
        todo!()
    }
}
