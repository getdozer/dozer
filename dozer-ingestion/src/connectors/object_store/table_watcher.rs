use crate::errors::ObjectStoreConnectorError::RecvError;
use std::sync::Arc;

use crate::{
    connectors::TableInfo,
    errors::{ConnectorError, ObjectStoreConnectorError},
    ingestion::Ingestor,
};
use deltalake::datafusion::prelude::SessionContext;
use dozer_types::{
    arrow_types::from_arrow::{map_schema_to_dozer, map_value_to_dozer_field},
    ingestion_types::{CsvConfig, DeltaConfig, IngestionMessage, ParquetConfig},
    types::{Operation, Record, SchemaIdentifier},
};
use futures::StreamExt;
use tokio::sync::mpsc::channel;
use tonic::async_trait;

use super::adapters::DozerObjectStore;

#[async_trait]
pub trait TableWatcher {
    async fn watch(
        &self,
        id: usize,
        table_info: &TableInfo,
        config: &impl DozerObjectStore,
        ingestor: &Ingestor,
    ) -> Result<(), ConnectorError>;
}

#[async_trait]
impl TableWatcher for CsvConfig {
    async fn watch(
        &self,
        id: usize,
        table_info: &TableInfo,
        config: &impl DozerObjectStore,
        ingestor: &Ingestor,
    ) -> Result<(), ConnectorError> {
        todo!()
    }
}

#[async_trait]
impl TableWatcher for DeltaConfig {
    async fn watch(
        &self,
        id: usize,
        table: &TableInfo,
        config: &impl DozerObjectStore,
        ingestor: &Ingestor,
    ) -> Result<(), ConnectorError> {
        ingestor
            .handle_message(IngestionMessage::new_snapshotting_started(0_u64, 0))
            .map_err(ObjectStoreConnectorError::IngestorError)?;

        let (tx, mut rx) = channel(16);

        let params = config.table_params(&table.name)?;

        tokio::spawn(async move {
            loop {
                let ctx = SessionContext::new();

                let delta_table = deltalake::open_table(&params.table_path).await.unwrap();
                //let cols: Vec<&str> = table.column_names.iter().map(|c| c.as_str()).collect();

                let data = ctx
                    .read_table(Arc::new(delta_table))
                    .unwrap()
                    //    .select_columns(&cols)?
                    .execute_stream()
                    .await
                    .unwrap();

                tokio::pin!(data);
                while let Some(Ok(batch)) = data.next().await {
                    let dozer_schema = map_schema_to_dozer(&batch.schema())
                        .map_err(|e| ConnectorError::InternalError(Box::new(e)))
                        .unwrap();
                    for row in 0..batch.num_rows() {
                        let fields = batch
                            .columns()
                            .iter()
                            .enumerate()
                            .map(|(col, column)| {
                                map_value_to_dozer_field(
                                    column,
                                    &row,
                                    dozer_schema.fields.get(col).unwrap().name.as_str(),
                                    &dozer_schema,
                                )
                                .unwrap()
                            })
                            .collect::<Vec<_>>();

                        let evt = Operation::Insert {
                            new: Record {
                                schema_id: Some(SchemaIdentifier {
                                    id: id as u32,
                                    version: 0,
                                }),
                                values: fields,
                                lifetime: None,
                            },
                        };

                        tx.send(Some(evt)).await.unwrap();
                    }
                }
                tx.send(None).await.unwrap();
            }
        });

        let mut idx = 1;
        loop {
            let message = rx
                .recv()
                .await
                .ok_or(ConnectorError::ObjectStoreConnectorError(RecvError))?;
            match message {
                None => {
                    //left_tables_count -= 1;
                    //if left_tables_count == 0 {
                    break;
                    //}
                }
                Some(evt) => {
                    ingestor
                        .handle_message(IngestionMessage::new_op(0, idx, evt))
                        .map_err(ConnectorError::IngestorError)?;
                    idx += 1;
                }
            }
        }

        ingestor
            .handle_message(IngestionMessage::new_snapshotting_done(0, idx))
            .map_err(ObjectStoreConnectorError::IngestorError)?;

        Ok(())
    }
}

#[async_trait]
impl TableWatcher for ParquetConfig {
    async fn watch(
        &self,
        id: usize,
        table: &TableInfo,
        config: &impl DozerObjectStore,
        ingestor: &Ingestor,
    ) -> Result<(), ConnectorError> {
        let params = config.table_params(&table.name)?;

        return Ok(());
    }
}
