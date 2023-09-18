use crate::connectors::object_store::adapters::DozerObjectStore;
use crate::connectors::TableInfo;
use crate::errors::ObjectStoreConnectorError::TableReaderError;
use crate::errors::ObjectStoreTableReaderError::{
    ColumnsSelectFailed, StreamExecutionError, TableReadFailed,
};
use crate::errors::{ConnectorError, ObjectStoreConnectorError};
use crate::ingestion::Ingestor;
use deltalake::datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};

use deltalake::datafusion::prelude::SessionContext;
use dozer_types::arrow_types::from_arrow::{map_schema_to_dozer, map_value_to_dozer_field};
use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::log::error;
use dozer_types::tonic::async_trait;
use dozer_types::types::{Operation, Record};
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

pub struct TableReader<T: Clone + Send + Sync> {
    pub(crate) config: T,
}

impl<T: Clone + Send + Sync> TableReader<T> {
    pub fn _new(config: T) -> TableReader<T> {
        Self { config }
    }

    pub async fn read(
        table_index: usize,
        ctx: SessionContext,
        table_path: ListingTableUrl,
        listing_options: ListingOptions,
        table: &TableInfo,
        sender: Sender<Result<Option<IngestionMessage>, ObjectStoreConnectorError>>,
    ) -> Result<(), ObjectStoreConnectorError> {
        let resolved_schema = listing_options
            .infer_schema(&ctx.state(), &table_path)
            .await
            .map_err(ObjectStoreConnectorError::InternalDataFusionError)?;

        let fields = resolved_schema.all_fields();

        let config = ListingTableConfig::new(table_path.clone())
            .with_listing_options(listing_options)
            .with_schema(resolved_schema.clone());

        let provider = Arc::new(
            ListingTable::try_new(config)
                .map_err(ObjectStoreConnectorError::InternalDataFusionError)?,
        );

        let cols: Vec<&str> = if table.column_names.is_empty() {
            fields.iter().map(|f| f.name().as_str()).collect()
        } else {
            table.column_names.iter().map(|c| c.as_str()).collect()
        };
        let data = ctx
            .read_table(provider.clone())
            .map_err(|e| TableReaderError(TableReadFailed(e)))?
            .select_columns(&cols)
            .map_err(|e| TableReaderError(ColumnsSelectFailed(e)))?
            .execute_stream()
            .await
            .map_err(|e| TableReaderError(StreamExecutionError(e)))?;

        tokio::pin!(data);

        while let Some(batch) = data.next().await {
            let batch = match batch {
                Ok(batch) => batch,
                Err(e) => {
                    error!("Error reading record batch from {table_path:?}: {e}");
                    continue;
                }
            };

            let batch_schema = batch.schema();
            let dozer_schema = map_schema_to_dozer(&batch_schema)?;

            for row in 0..batch.num_rows() {
                let fields = batch
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(col, column)| {
                        map_value_to_dozer_field(
                            column,
                            row,
                            resolved_schema.field(col).name(),
                            &dozer_schema,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let evt = Operation::Insert {
                    new: Record {
                        values: fields,
                        lifetime: None,
                    },
                };

                if sender
                    .send(Ok(Some(IngestionMessage::OperationEvent {
                        table_index,
                        op: evt,
                        id: None,
                    })))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }

        // sender.send(Ok(None)).await.unwrap();

        Ok(())
    }
}

#[async_trait]
pub trait Reader<T> {
    async fn read_tables(
        &self,
        tables: &[TableInfo],
        ingestor: &Ingestor,
    ) -> Result<(), ConnectorError>;
}

#[async_trait]
impl<T: DozerObjectStore> Reader<T> for TableReader<T> {
    async fn read_tables(
        &self,
        _tables: &[TableInfo],
        _ingestor: &Ingestor,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }
}
