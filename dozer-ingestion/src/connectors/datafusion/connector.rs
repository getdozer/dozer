use crossbeam::channel;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use std::collections::HashMap;
use std::sync::Arc;

use crate::connectors::datafusion::schema_helper::{map_schema_to_dozer, map_value_to_dozer_field};
use crate::connectors::TableInfo;
use crate::errors::{ConnectorError, DataFusionConnectorError};
use crate::{connectors::Connector, errors, ingestion::Ingestor};
use datafusion::prelude::SessionContext;
use dozer_types::ingestion_types::{IngestionMessage, LocalStorage, S3Storage};
use dozer_types::log::error;
use dozer_types::parking_lot::RwLock;
use dozer_types::types::ReplicationChangesTrackingType::Nothing;
use dozer_types::types::{Operation, OperationEvent, Record, Schema, SchemaIdentifier};
use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use tokio::runtime::Runtime;

pub struct DataFusionConnector<T: Clone> {
    pub id: u64,
    config: T,
    ingestor: Option<Arc<RwLock<Ingestor>>>,
    tables: Option<Vec<TableInfo>>,
}

impl<T: Clone> DataFusionConnector<T> {
    pub fn new(id: u64, config: T) -> Self {
        Self {
            id,
            config,
            ingestor: None,
            tables: None,
        }
    }
}

impl Connector for DataFusionConnector<S3Storage> {
    fn test_connection(&self) -> Result<(), ConnectorError> {
        todo!()
    }

    fn validate(&self, _tables: Option<Vec<TableInfo>>) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn validate_schemas(
        &self,
        _tables: &[crate::connectors::TableInfo],
    ) -> Result<crate::connectors::ValidationResults, errors::ConnectorError> {
        Ok(HashMap::new())
    }

    fn get_schemas(
        &self,
        table_names: Option<Vec<TableInfo>>,
    ) -> Result<Vec<dozer_types::types::SchemaWithChangesType>, ConnectorError> {
        let details = self.config.details.as_ref().unwrap();
        let used_tables: Vec<String> = table_names
            .unwrap()
            .iter()
            .map(|t| t.table_name.clone())
            .collect();

        let mut schemas = vec![];
        for table in self.config.tables.clone() {
            if used_tables.contains(&table.name) {
                let path = format!("s3://{}/{}/", details.bucket_name, table.folder_name);

                let table_path = ListingTableUrl::parse(path).unwrap();

                let listing_options = match table.file_type.as_str() {
                    "parquet" => {
                        let format = ParquetFormat::new();
                        ListingOptions::new(Arc::new(format)).with_file_extension(table.extension)
                    }
                    "csv" => {
                        let format = CsvFormat::default();
                        ListingOptions::new(Arc::new(format)).with_file_extension(table.extension)
                    }
                    _ => {
                        let format = ParquetFormat::new();
                        ListingOptions::new(Arc::new(format)).with_file_extension(table.extension)
                    }
                };

                let (tx, rx) = channel::bounded(1);

                let rt = Runtime::new().unwrap();

                rt.block_on(async move {
                    let ctx = SessionContext::new();
                    let s3 = AmazonS3Builder::new()
                        .with_bucket_name(details.bucket_name.to_owned())
                        .with_region(details.region.to_owned())
                        .with_access_key_id(details.access_key_id.to_owned())
                        .with_secret_access_key(details.secret_access_key.to_owned())
                        .build()
                        .map_or(Err(ConnectorError::InitializationError), Ok)
                        .unwrap();

                    ctx.runtime_env().register_object_store(
                        "s3",
                        &details.bucket_name,
                        Arc::new(s3),
                    );

                    let resolved_schema = listing_options
                        .infer_schema(&ctx.state(), &table_path)
                        .await;

                    tx.send(resolved_schema).unwrap();
                });

                let c = rx
                    .recv()
                    .map_err(|e| {
                        error!("{:?}", e);
                        ConnectorError::WrongConnectionConfiguration
                    })?
                    .map_err(|e| {
                        error!("{:?}", e);
                        ConnectorError::WrongConnectionConfiguration
                    })?;

                // let mut cols = vec![];
                // let fields_list = match table.columns {
                //     Some(columns) if !columns.is_empty() => {
                //         for f in c.fields() {
                //             if columns.contains(f.name()) {
                //                 cols.push(f.clone());
                //             }
                //         }
                //         cols.as_ref()
                //     }
                //     _ => c.fields(),
                // };

                let fields_list = c.fields();
                let fields = map_schema_to_dozer(fields_list).map_err(|e| {
                    ConnectorError::DataFusionConnectorError(
                        DataFusionConnectorError::DataFusionSchemaError(e),
                    )
                })?;
                let schema = Schema {
                    identifier: Some(SchemaIdentifier { id: 0, version: 0 }),
                    fields,
                    primary_index: vec![],
                };
                schemas.push((table.name.clone(), schema, Nothing))
            }
        }

        Ok(schemas)
    }

    fn get_tables(&self) -> Result<Vec<crate::connectors::TableInfo>, errors::ConnectorError> {
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

    fn start(&self, _from_seq: Option<(u64, u64)>) -> Result<(), ConnectorError> {
        let details = self.config.details.as_ref().unwrap();
        let tables = self
            .tables
            .as_ref()
            .map_or_else(std::vec::Vec::new, |tables| tables.clone());

        let used_tables: Vec<String> = tables.iter().map(|t| t.table_name.clone()).collect();

        for table in self.config.tables.clone() {
            if used_tables.contains(&table.name) {
                let mut idx = 0;
                let ingestor = self.ingestor.as_ref().unwrap();
                let path = format!("s3://{}/{}/", details.bucket_name, table.folder_name);

                let table_path = ListingTableUrl::parse(path).unwrap();

                let file_format = ParquetFormat::new();
                let listing_options =
                    ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet");

                let rt = Runtime::new().unwrap();

                rt.block_on(async move {
                    let ctx = SessionContext::new();
                    let s3 = AmazonS3Builder::new()
                        .with_bucket_name(details.bucket_name.to_owned())
                        .with_region(details.region.to_owned())
                        .with_access_key_id(details.access_key_id.to_owned())
                        .with_secret_access_key(details.secret_access_key.to_owned())
                        .build()
                        .map_or(Err(ConnectorError::InitializationError), Ok)
                        .unwrap();

                    ctx.runtime_env().register_object_store(
                        "s3",
                        &details.bucket_name,
                        Arc::new(s3),
                    );

                    let resolved_schema = listing_options
                        .infer_schema(&ctx.state(), &table_path)
                        .await
                        .unwrap();

                    let fields = resolved_schema.all_fields();

                    let config = ListingTableConfig::new(table_path)
                        .with_listing_options(listing_options)
                        .with_schema(resolved_schema.clone());

                    let provider = Arc::new(ListingTable::try_new(config).unwrap());

                    // let columns: Vec<String> = match table.columns {
                    //     Some(columns_list) if !columns_list.is_empty() => columns_list.to_vec(),
                    //     _ => fields.iter().map(|f| f.name().clone()).collect(),
                    // };

                    let columns: Vec<String> = fields.iter().map(|f| f.name().clone()).collect();
                    let cols: Vec<&str> = columns.iter().map(String::as_str).collect();
                    let data = ctx
                        .read_table(provider.clone())
                        .unwrap()
                        .select_columns(&cols)
                        .unwrap()
                        .execute_stream()
                        .await
                        .unwrap();

                    tokio::pin!(data);
                    loop {
                        let item = data.next().await;
                        if let Some(Ok(batch)) = item {
                            for row in 0..batch.num_rows() {
                                let mut fields = vec![];
                                for col in 0..batch.num_columns() {
                                    let column = batch.column(col);
                                    fields.push(
                                        map_value_to_dozer_field(
                                            column,
                                            &row,
                                            resolved_schema.field(col).name(),
                                        )
                                        .unwrap(),
                                    );
                                }

                                ingestor
                                    .write()
                                    .handle_message((
                                        (0_u64, idx),
                                        IngestionMessage::OperationEvent(OperationEvent {
                                            seq_no: idx,
                                            operation: Operation::Insert {
                                                new: Record {
                                                    schema_id: Some(SchemaIdentifier {
                                                        id: 0,
                                                        version: 0,
                                                    }),
                                                    values: fields,
                                                    version: None,
                                                },
                                            },
                                        }),
                                    ))
                                    .unwrap();

                                idx += 1;
                            }
                        }
                    }
                });
            }
        }

        Ok(())
    }

    fn stop(&self) {
        todo!()
    }
}

impl Connector for DataFusionConnector<LocalStorage> {
    fn test_connection(&self) -> Result<(), ConnectorError> {
        todo!()
    }

    fn validate(&self, _tables: Option<Vec<TableInfo>>) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn validate_schemas(
        &self,
        _tables: &[crate::connectors::TableInfo],
    ) -> Result<crate::connectors::ValidationResults, errors::ConnectorError> {
        Ok(HashMap::new())
    }

    fn get_schemas(
        &self,
        table_names: Option<Vec<TableInfo>>,
    ) -> Result<Vec<dozer_types::types::SchemaWithChangesType>, ConnectorError> {
        let details = self.config.details.as_ref().unwrap();
        let used_tables: Vec<String> = table_names
            .unwrap()
            .iter()
            .map(|t| t.table_name.clone())
            .collect();

        let mut schemas = vec![];
        for table in self.config.tables.clone() {
            if used_tables.contains(&table.name) {
                let path = format!("{}/{}/", details.path, table.folder_name);

                let listing_options = match table.file_type.as_str() {
                    "parquet" => {
                        let format = ParquetFormat::new();
                        ListingOptions::new(Arc::new(format)).with_file_extension(table.extension)
                    }
                    "csv" => {
                        let format = CsvFormat::default();
                        ListingOptions::new(Arc::new(format)).with_file_extension(table.extension)
                    }
                    _ => {
                        let format = ParquetFormat::new();
                        ListingOptions::new(Arc::new(format)).with_file_extension(table.extension)
                    }
                };

                let (tx, rx) = channel::bounded(1);

                let rt = Runtime::new().unwrap();

                rt.block_on(async move {
                    let ctx = SessionContext::new();
                    let ls = LocalFileSystem::new_with_prefix(details.path.clone()).unwrap();

                    ctx.runtime_env()
                        .register_object_store("local", &details.path, Arc::new(ls));

                    let table_path = ListingTableUrl::parse(path.clone()).unwrap();
                    let resolved_schema = listing_options
                        .infer_schema(&ctx.state(), &table_path)
                        .await;

                    tx.send(resolved_schema).unwrap();
                });

                let c = rx
                    .recv()
                    .map_err(|e| {
                        error!("{:?}", e);
                        ConnectorError::WrongConnectionConfiguration
                    })?
                    .map_err(|e| {
                        error!("{:?}", e);
                        ConnectorError::WrongConnectionConfiguration
                    })?;

                // let mut cols = vec![];
                // let fields_list = match table.columns {
                //     Some(columns) if !columns.is_empty() => {
                //         for f in c.fields() {
                //             if columns.contains(f.name()) {
                //                 cols.push(f.clone());
                //             }
                //         }
                //         cols.as_ref()
                //     }
                //     _ => c.fields(),
                // };

                let fields_list = c.fields();
                let fields = map_schema_to_dozer(fields_list).map_err(|e| {
                    ConnectorError::DataFusionConnectorError(
                        DataFusionConnectorError::DataFusionSchemaError(e),
                    )
                })?;
                let schema = Schema {
                    identifier: Some(SchemaIdentifier { id: 0, version: 0 }),
                    fields,
                    primary_index: vec![],
                };
                schemas.push((table.name.clone(), schema, Nothing))
            }
        }

        Ok(schemas)
    }

    fn get_tables(&self) -> Result<Vec<crate::connectors::TableInfo>, errors::ConnectorError> {
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

    fn start(&self, _from_seq: Option<(u64, u64)>) -> Result<(), ConnectorError> {
        let details = self.config.details.as_ref().unwrap();
        let tables = self
            .tables
            .as_ref()
            .map_or_else(std::vec::Vec::new, |tables| tables.clone());

        let used_tables: Vec<String> = tables.iter().map(|t| t.table_name.clone()).collect();

        for table in self.config.tables.clone() {
            if used_tables.contains(&table.name) {
                let mut idx = 0;
                let ingestor = self.ingestor.as_ref().unwrap();
                let path = format!("{}/{}/", details.path, table.folder_name);

                // let table_path = ListingTableUrl::parse(path).unwrap();

                let listing_options = match table.file_type.as_str() {
                    "parquet" => {
                        let format = ParquetFormat::new();
                        ListingOptions::new(Arc::new(format)).with_file_extension(table.extension)
                    }
                    "csv" => {
                        let format = CsvFormat::default();
                        ListingOptions::new(Arc::new(format)).with_file_extension(table.extension)
                    }
                    _ => {
                        let format = ParquetFormat::new();
                        ListingOptions::new(Arc::new(format)).with_file_extension(table.extension)
                    }
                };

                let rt = Runtime::new().unwrap();

                rt.block_on(async move {
                    let ctx = SessionContext::new();
                    let ls = LocalFileSystem::new_with_prefix(details.path.clone()).unwrap();

                    ctx.runtime_env()
                        .register_object_store("local", &details.path, Arc::new(ls));

                    let table_path = ListingTableUrl::parse(path.clone()).unwrap();
                    let resolved_schema = listing_options
                        .infer_schema(&ctx.state(), &table_path)
                        .await
                        .unwrap();

                    let fields = resolved_schema.all_fields();

                    let config = ListingTableConfig::new(table_path)
                        .with_listing_options(listing_options)
                        .with_schema(resolved_schema.clone());

                    let provider = Arc::new(ListingTable::try_new(config).unwrap());

                    // let columns: Vec<String> = match table.columns {
                    //     Some(columns_list) if !columns_list.is_empty() => columns_list.to_vec(),
                    //     _ => fields.iter().map(|f| f.name().clone()).collect(),
                    // };

                    let columns: Vec<String> = fields.iter().map(|f| f.name().clone()).collect();

                    let cols: Vec<&str> = columns.iter().map(String::as_str).collect();
                    let data = ctx
                        .read_table(provider.clone())
                        .unwrap()
                        .select_columns(&cols)
                        .unwrap()
                        .execute_stream()
                        .await
                        .unwrap();

                    tokio::pin!(data);
                    loop {
                        let item = data.next().await;
                        if let Some(Ok(batch)) = item {
                            for row in 0..batch.num_rows() {
                                let mut fields = vec![];
                                for col in 0..batch.num_columns() {
                                    let column = batch.column(col);
                                    fields.push(
                                        map_value_to_dozer_field(
                                            column,
                                            &row,
                                            resolved_schema.field(col).name(),
                                        )
                                        .unwrap(),
                                    );
                                }

                                ingestor
                                    .write()
                                    .handle_message((
                                        (0_u64, idx),
                                        IngestionMessage::OperationEvent(OperationEvent {
                                            seq_no: idx,
                                            operation: Operation::Insert {
                                                new: Record {
                                                    schema_id: Some(SchemaIdentifier {
                                                        id: 0,
                                                        version: 0,
                                                    }),
                                                    values: fields,
                                                    version: None,
                                                },
                                            },
                                        }),
                                    ))
                                    .unwrap();

                                idx += 1;
                            }
                        }
                    }
                });
            }
        }

        Ok(())
    }

    fn stop(&self) {
        todo!()
    }
}
