use std::sync::Arc;

use crate::{connectors::TableInfo, errors::ConnectorError};
use deltalake::datafusion::prelude::SessionContext;
use dozer_types::{
    arrow_types::from_arrow::{map_schema_to_dozer, map_value_to_dozer_field},
    ingestion_types::{CsvConfig, DeltaConfig, ParquetConfig},
};
use futures::StreamExt;
use tonic::async_trait;

#[async_trait]
pub trait TableWatcher {
    async fn watch(&self, id: usize, table_info: &TableInfo) -> Result<(), ConnectorError>;
}

#[async_trait]
impl TableWatcher for CsvConfig {
    async fn watch(&self, id: usize, table_info: &TableInfo) -> Result<(), ConnectorError> {
        todo!()
    }
}

#[async_trait]
impl TableWatcher for DeltaConfig {
    async fn watch(&self, id: usize, table_info: &TableInfo) -> Result<(), ConnectorError> {
        let ctx = SessionContext::new();

        let delta_table = deltalake::open_table(&self.path).await?;
        let cols: Vec<&str> = table_info.column_names.iter().map(|c| c.as_str()).collect();

        let data = ctx
            .read_table(Arc::new(delta_table))?
            .select_columns(&cols)?
            .execute_stream()
            .await?;

        tokio::pin!(data);
        while let Some(Ok(batch)) = data.next().await {
            let dozer_schema = map_schema_to_dozer(&batch.schema())
                .map_err(|e| ConnectorError::InternalError(Box::new(e)))?;
            for row in 0..batch.num_rows() {
                let fields = batch
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(col, column)| {
                        map_value_to_dozer_field(column, &row, cols[col], &dozer_schema).unwrap()
                    })
                    .collect::<Vec<_>>();
            }
        }
        Ok(())
    }
}

#[async_trait]
impl TableWatcher for ParquetConfig {
    async fn watch(&self, id: usize, table_info: &TableInfo) -> Result<(), ConnectorError> {
        let ctx = SessionContext::new();
        // match connection {
        //     ConnectionConfig::S3Storage(s3) => {
        //         if let Some(details) = &s3.details {
        //             let s3_store = AmazonS3Builder::new()
        //                 .with_bucket_name(&details.bucket_name)
        //                 .with_region(&details.region)
        //                 .with_access_key_id(&details.access_key_id)
        //                 .with_secret_access_key(&details.secret_access_key)
        //                 .build()
        //                 .map_err(|e| ConnectorError::InitializationError(e.to_string()))?;
        //         } else {
        //             return Err(ConnectorError::InitializationError(
        //                 "S3 details not provided".to_string(),
        //             ));
        //         }
        //     }

        //     ConnectionConfig::LocalStorage(local) => {
        //         if let Some(details) = &local.details {
        //             let local_store = LocalFileSystem::new_with_prefix(details.path.as_str())
        //                 .map_err(|e| ConnectorError::InitializationError(e.to_string()))?;
        //         } else {
        //             return Err(ConnectorError::InitializationError(
        //                 "Local Storage details not provided".to_string(),
        //             ));
        //         }
        //     }
        //     _ => todo!(),
        // };

        return Ok(());
    }
}
