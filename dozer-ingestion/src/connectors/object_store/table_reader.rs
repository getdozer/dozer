use crate::connectors::object_store::adapters::DozerObjectStore;
use crate::connectors::object_store::helper::map_listing_options;
use crate::connectors::object_store::schema_helper::map_value_to_dozer_field;
use crate::connectors::TableInfo;
use crate::errors::ObjectStoreConnectorError::TableReaderError;
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
use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::log::error;
use dozer_types::types::{Operation, Record, SchemaIdentifier};
use futures::StreamExt;
use std::sync::Arc;
use tokio::runtime::Runtime;

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
        ingestor: &Ingestor,
        table: &TableInfo,
    ) -> Result<(), ObjectStoreConnectorError> {
        let resolved_schema = listing_options
            .infer_schema(&ctx.state(), &table_path)
            .await
            .map_err(ObjectStoreConnectorError::InternalDataFusionError)?;

        let mut idx = 0;
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

            for row in 0..batch.num_rows() {
                let fields = batch
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(col, column)| {
                        map_value_to_dozer_field(column, &row, resolved_schema.field(col).name())
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                ingestor
                    .handle_message(IngestionMessage::new_op(
                        0_u64,
                        idx,
                        Operation::Insert {
                            new: Record {
                                schema_id: Some(SchemaIdentifier { id, version: 0 }),
                                values: fields,
                                version: None,
                            },
                        },
                    ))
                    .map_err(ObjectStoreConnectorError::IngestorError)?;

                idx += 1;
            }
        }

        Ok(())
    }
}

pub trait Reader<T> {
    fn read_tables(&self, tables: &[TableInfo], ingestor: &Ingestor) -> Result<(), ConnectorError>;
}

impl<T: DozerObjectStore> Reader<T> for TableReader<T> {
    fn read_tables(&self, tables: &[TableInfo], ingestor: &Ingestor) -> Result<(), ConnectorError> {
        ingestor
            .handle_message(IngestionMessage::new_snapshotting_started(0_u64, 0))
            .map_err(ObjectStoreConnectorError::IngestorError)?;
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

            let rt = Runtime::new().map_err(|_| ObjectStoreConnectorError::RuntimeCreationError)?;

            let ctx = SessionContext::new();

            ctx.runtime_env().register_object_store(
                params.scheme,
                params.host,
                Arc::new(params.object_store),
            );

            rt.block_on(Self::read(
                (id + 1) as u32,
                ctx,
                table_path,
                listing_options,
                ingestor,
                table,
            ))?;
        }

        ingestor
            .handle_message(IngestionMessage::new_snapshotting_done(
                (tables.len() + 1) as u64,
                0,
            ))
            .map_err(ObjectStoreConnectorError::IngestorError)?;

        Ok(())
    }
}
