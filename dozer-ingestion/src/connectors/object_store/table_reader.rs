use crate::connectors::object_store::helper::{get_details, get_table, map_listing_options};
use crate::connectors::object_store::schema_helper::map_value_to_dozer_field;
use crate::connectors::{ColumnInfo, TableInfo};
use crate::errors::ObjectStoreConnectorError::TableReaderError;
use crate::errors::ObjectStoreObjectError::ListingPathParsingError;
use crate::errors::ObjectStoreTableReaderError::{
    ColumnsSelectFailed, StreamExecutionError, TableReadFailed,
};
use crate::errors::{ConnectorError, ObjectStoreConnectorError};
use crate::ingestion::Ingestor;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::SessionContext;
use dozer_types::ingestion_types::{IngestionMessage, LocalStorage, S3Storage, Table};
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Operation, Record, SchemaIdentifier};
use futures::StreamExt;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
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
        ingestor: Arc<RwLock<Ingestor>>,
        table: &TableInfo,
    ) -> Result<(), ObjectStoreConnectorError> {
        let resolved_schema = listing_options
            .infer_schema(&ctx.state(), &table_path)
            .await
            .map_err(|_| ObjectStoreConnectorError::InternalError)?;

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
                    .write()
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

pub trait DozerObjectStore: Clone + Send + Sync {
    type ObjectStore: ObjectStore;

    fn table_params<'a>(
        &'a self,
        table_name: &str,
    ) -> Result<DozerObjectStoreParams<'a, Self::ObjectStore>, ConnectorError>;
}

pub struct DozerObjectStoreParams<'a, T: ObjectStore> {
    pub scheme: &'static str,
    pub host: &'a str,
    pub object_store: T,
    pub table_path: String,
    pub data_fusion_table: &'a Table,
}

pub trait Reader<T> {
    fn read_tables(
        &self,
        tables: &[TableInfo],
        ingestor: &Arc<RwLock<Ingestor>>,
    ) -> Result<(), ConnectorError>;
}

impl<T: DozerObjectStore> Reader<T> for TableReader<T> {
    fn read_tables(
        &self,
        tables: &[TableInfo],
        ingestor: &Arc<RwLock<Ingestor>>,
    ) -> Result<(), ConnectorError> {
        for (id, table) in tables.iter().enumerate() {
            let params = self.config.table_params(&table.name)?;

            let table_path = ListingTableUrl::parse(params.table_path).map_err(|_| {
                ObjectStoreConnectorError::DataFusionStorageObjectError(ListingPathParsingError)
            })?;

            let listing_options = map_listing_options(params.data_fusion_table);

            let rt = Runtime::new().map_err(|_| ObjectStoreConnectorError::RuntimeCreationError)?;

            let ctx = SessionContext::new();

            ctx.runtime_env().register_object_store(
                params.scheme,
                params.host,
                Arc::new(params.object_store),
            );

            rt.block_on(Self::read(
                id as u32,
                ctx,
                table_path,
                listing_options,
                ingestor.clone(),
                table,
            ))?;
        }

        Ok(())
    }
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
}
