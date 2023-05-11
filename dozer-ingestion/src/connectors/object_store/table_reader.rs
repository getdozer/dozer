use crate::connectors::object_store::adapters::DozerObjectStore;
use crate::connectors::object_store::helper::map_listing_options;
use crate::connectors::TableInfo;
use crate::errors::ObjectStoreConnectorError::{RecvError, TableReaderError};
use crate::errors::ObjectStoreObjectError::ListingPathParsingError;
use crate::errors::ObjectStoreTableReaderError::{
    ColumnsSelectFailed, StreamExecutionError, TableReadFailed,
};
use crate::errors::{ConnectorError, ObjectStoreConnectorError};
use crate::ingestion::Ingestor;
use deltalake::datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use deltalake::datafusion::prelude::SessionContext;
use deltalake::Path;
use dozer_types::arrow_types::from_arrow::map_value_to_dozer_field;
use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::log::error;
use dozer_types::types::{Operation, Record, SchemaIdentifier};
use futures::StreamExt;
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{channel, Sender};
use tonic::async_trait;

pub struct TableReader<T: Clone + Send + Sync> {
    config: T,
}

impl<T: Clone + Send + Sync> TableReader<T> {
    pub fn new(config: T) -> TableReader<T> {
        Self { config }
    }

    pub async fn read(
        id: u32,
        ctx: SessionContext,
        table_path: ListingTableUrl,
        listing_options: ListingOptions,
        table: &TableInfo,
        sender: Sender<Result<Option<Operation>, ObjectStoreConnectorError>>,
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
            let metadata = batch_schema.metadata();

            for row in 0..batch.num_rows() {
                let fields = batch
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(col, column)| {
                        map_value_to_dozer_field(
                            column,
                            &row,
                            resolved_schema.field(col).name(),
                            metadata,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let evt = Operation::Insert {
                    new: Record {
                        schema_id: Some(SchemaIdentifier { id, version: 0 }),
                        values: fields,
                        lifetime: None,
                    },
                };

                sender.send(Ok(Some(evt))).await.unwrap();
            }
        }

        sender.send(Ok(None)).await.unwrap();

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
        tables: &[TableInfo],
        ingestor: &Ingestor,
    ) -> Result<(), ConnectorError> {
        ingestor
            .handle_message(IngestionMessage::new_snapshotting_started(0_u64, 0))
            .map_err(ObjectStoreConnectorError::IngestorError)?;

        let mut left_tables_count = tables.len();
        let (tx, mut rx) = channel(16);

        for (id, table) in tables.iter().enumerate() {
            let params = self.config.table_params(&table.name)?;

            let table_path = ListingTableUrl::parse(&params.table_path).map_err(|e| {
                ObjectStoreConnectorError::DataFusionStorageObjectError(ListingPathParsingError(
                    params.table_path.clone(),
                    e,
                ))
            })?;

            let listing_options = map_listing_options(params.data_fusion_table)
                .map_err(ObjectStoreConnectorError::DataFusionStorageObjectError)?;

            let ctx = SessionContext::new();

            let store = Arc::new(params.object_store);
            ctx.runtime_env()
                .register_object_store(params.scheme, params.host, store.clone());

            let sender = tx.clone();
            let t = table.clone();

            self.watch(id as u32, table, sender).await.unwrap();

            // tokio::spawn(async move {
            //     let result = Self::read(
            //         id as u32,
            //         ctx,
            //         table_path,
            //         listing_options,
            //         &t,
            //         sender.clone(),
            //     )
            //     .await;
            //     if let Err(e) = result {
            //         sender.send(Err(e)).await.unwrap();
            //     }
            // });
        }

        let mut idx = 1;
        loop {
            let message = rx
                .recv()
                .await
                .ok_or(ConnectorError::ObjectStoreConnectorError(RecvError))??;
            match message {
                None => {
                    left_tables_count -= 1;
                    if left_tables_count == 0 {
                        break;
                    }
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
pub trait Watcher<T> {
    async fn watch(
        &self,
        id: u32,
        table: &TableInfo,
        sender: Sender<Result<Option<Operation>, ObjectStoreConnectorError>>,
    ) -> Result<(), ConnectorError>;
}

#[async_trait]
impl<T: DozerObjectStore> Watcher<T> for TableReader<T> {
    async fn watch(
        &self,
        id: u32,
        table: &TableInfo,
        sender: Sender<Result<Option<Operation>, ObjectStoreConnectorError>>,
    ) -> Result<(), ConnectorError> {
        let params = self.config.table_params(&table.name)?;
        let store = Arc::new(params.object_store);

        tokio::spawn(async move {
            loop {
                // List objects in the S3 bucket with the specified prefix
                let mut stream = store.list(Some(&Path::from("taxi_data"))).await.unwrap();

                while let Some(item) = stream.next().await {
                    // Check if any objects have been added or modified
                    let object = item.unwrap();
                    println!(
                        "Found object: {:?}, {:?}",
                        object.location, object.last_modified
                    );

                    // Do something with the object, such as download it or process it
                }

                let table_path = ListingTableUrl::parse("data/trips")
                    .map_err(|e| {
                        ObjectStoreConnectorError::DataFusionStorageObjectError(
                            ListingPathParsingError(params.table_path.clone(), e),
                        )
                    })
                    .unwrap();

                // Wait for 10 seconds before checking again
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        });

        Ok(())
    }
}
