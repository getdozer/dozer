use crate::connectors::object_store::helper::map_listing_options;
use crate::connectors::object_store::schema_helper::map_value_to_dozer_field;
use crate::connectors::{ColumnInfo, TableInfo};
use crate::errors::ObjectStoreConnectorError::TableReaderError;
use crate::errors::ObjectStoreObjectError::{
    ListingPathParsingError, MissingStorageDetails, TableDefinitionNotFound,
};
use crate::errors::ObjectStoreTableReaderError::{
    ColumnsSelectFailed, StreamExecutionError, TableReadFailed,
};
use crate::errors::{ConnectorError, ObjectStoreConnectorError};
use crate::ingestion::Ingestor;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::SessionContext;
use dozer_types::ingestion_types::{IngestionMessage, LocalStorage, S3Storage, Table};
use dozer_types::types::{Operation, Record, SchemaIdentifier};
use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use std::collections::HashMap;
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
        resolved_schema: SchemaRef,
        table_path: ListingTableUrl,
        listing_options: ListingOptions,
        ingestor: &Ingestor,
        table: &TableInfo,
    ) -> Result<(), ObjectStoreConnectorError> {
        let mut idx = 0;
        let fields = resolved_schema.all_fields();

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(resolved_schema.clone());

        let provider = Arc::new(
            ListingTable::try_new(config)
                .map_err(ObjectStoreConnectorError::InternalDataFusionError)?,
        );

        let columns: Vec<ColumnInfo> = match &table.columns {
            Some(columns_list) if !columns_list.is_empty() => columns_list.clone(),
            _ => fields
                .iter()
                .map(|f| ColumnInfo {
                    name: f.name().to_string(),
                    data_type: Some(f.data_type().to_string()),
                })
                .collect(),
        };

        let cols: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        let data = ctx
            .read_table(provider.clone())
            .map_err(|e| TableReaderError(TableReadFailed(e)))?
            .select_columns(&cols)
            .map_err(|e| TableReaderError(ColumnsSelectFailed(e)))?
            .execute_stream()
            .await
            .map_err(|e| TableReaderError(StreamExecutionError(e)))?;

        tokio::pin!(data);

        while let Some(Ok(batch)) = data.next().await {
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
                    .handle_message((
                        (0_u64, idx),
                        IngestionMessage::OperationEvent(Operation::Insert {
                            new: Record {
                                schema_id: Some(SchemaIdentifier { id, version: 0 }),
                                values: fields,
                                version: None,
                            },
                        }),
                    ))
                    .unwrap();

                idx += 1;
            }
        }

        Ok(())
    }
}

pub trait Reader<T> {
    fn read_tables(
        &self,
        tables: Vec<TableInfo>,
        ingestor: &Ingestor,
    ) -> Result<(), ConnectorError>;
}

impl Reader<S3Storage> for TableReader<S3Storage> {
    fn read_tables(
        &self,
        tables: Vec<TableInfo>,
        ingestor: &Ingestor,
    ) -> Result<(), ConnectorError> {
        let tables_map = get_tables_map(&self.config.tables);
        let details = get_details(&self.config.details)?;

        for (id, table) in tables.iter().enumerate() {
            let data_fusion_table = tables_map.get(&table.table_name).ok_or(
                ObjectStoreConnectorError::DataFusionStorageObjectError(TableDefinitionNotFound),
            )?;

            let path = format!("s3://{}/{}/", details.bucket_name, data_fusion_table.prefix);
            let table_path = get_table_path(path)?;

            let listing_options = map_listing_options(data_fusion_table);

            let rt = Runtime::new().map_err(|_| ObjectStoreConnectorError::RuntimeCreationError)?;

            let ctx = SessionContext::new();
            let s3 = AmazonS3Builder::new()
                .with_bucket_name(&details.bucket_name)
                .with_region(&details.region)
                .with_access_key_id(&details.access_key_id)
                .with_secret_access_key(&details.secret_access_key)
                .build()
                .map_err(|_| ConnectorError::InitializationError)?;

            ctx.runtime_env()
                .register_object_store("s3", &details.bucket_name, Arc::new(s3));

            rt.block_on(async move {
                let resolved_schema = listing_options
                    .infer_schema(&ctx.state(), &table_path)
                    .await
                    .map_err(|_| ObjectStoreConnectorError::InternalError)?;

                Self::read(
                    id as u32,
                    ctx,
                    resolved_schema,
                    table_path,
                    listing_options,
                    ingestor,
                    table,
                )
                .await
            })
            .map_err(ConnectorError::DataFusionConnectorError)?;
        }

        Ok(())
    }
}

impl Reader<LocalStorage> for TableReader<LocalStorage> {
    fn read_tables(
        &self,
        tables: Vec<TableInfo>,
        ingestor: &Ingestor,
    ) -> Result<(), ConnectorError> {
        let tables_map = get_tables_map(&self.config.tables);
        let details = get_details(&self.config.details)?;

        for (id, table) in tables.iter().enumerate() {
            let data_fusion_table = get_table(&tables_map, table)?;

            let path = format!("{}/{}/", details.path, data_fusion_table.prefix);
            let table_path = get_table_path(path)?;

            let listing_options = map_listing_options(data_fusion_table);

            let rt = Runtime::new().map_err(|_| ObjectStoreConnectorError::RuntimeCreationError)?;

            let ctx = SessionContext::new();
            let ls = LocalFileSystem::new_with_prefix(&details.path)
                .map_err(|_| ConnectorError::InitializationError)?;

            ctx.runtime_env()
                .register_object_store("local", &details.path, Arc::new(ls));

            rt.block_on(async move {
                let resolved_schema = listing_options
                    .infer_schema(&ctx.state(), &table_path)
                    .await
                    .map_err(|_| ObjectStoreConnectorError::InternalError)?;

                Self::read(
                    id as u32,
                    ctx,
                    resolved_schema,
                    table_path,
                    listing_options,
                    ingestor,
                    table,
                )
                .await
            })?;
        }

        Ok(())
    }
}

fn get_details<T>(details: &Option<T>) -> Result<&T, ObjectStoreConnectorError> {
    details
        .as_ref()
        .ok_or(ObjectStoreConnectorError::DataFusionStorageObjectError(
            MissingStorageDetails,
        ))
}

fn get_tables_map(tables: &[Table]) -> HashMap<String, Table> {
    tables
        .iter()
        .cloned()
        .map(|table| (table.name.clone(), table))
        .collect()
}

fn get_table_path(path: String) -> Result<ListingTableUrl, ObjectStoreConnectorError> {
    ListingTableUrl::parse(path).map_err(|_| {
        ObjectStoreConnectorError::DataFusionStorageObjectError(ListingPathParsingError)
    })
}

fn get_table<'a>(
    tables_map: &'a HashMap<String, Table>,
    table: &TableInfo,
) -> Result<&'a Table, ObjectStoreConnectorError> {
    tables_map.get(&table.table_name).ok_or(
        ObjectStoreConnectorError::DataFusionStorageObjectError(TableDefinitionNotFound),
    )
}
