use crate::connectors::datafusion::helper::map_listing_options;
use crate::connectors::datafusion::schema_helper::map_schema_to_dozer;
use crate::connectors::TableInfo;
use crate::errors::DataFusionStorageObjectError::{
    ListingPathParsingError, MissingStorageDetails, TableDefinitionNotFound,
};
use crate::errors::{ConnectorError, DataFusionConnectorError};
use crossbeam::channel;
use crossbeam::channel::Receiver;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::prelude::SessionContext;
use dozer_types::ingestion_types::{DataFusionTable, LocalStorage, S3Storage};
use dozer_types::log::error;
use dozer_types::types::ReplicationChangesTrackingType::Nothing;
use dozer_types::types::{Schema, SchemaIdentifier, SchemaWithChangesType};
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;

pub struct SchemaMapper<T: Clone + Send + Sync> {
    config: T,
}

impl<T: Clone + Send + Sync> SchemaMapper<T> {
    pub fn new(config: T) -> SchemaMapper<T> {
        Self { config }
    }

    fn map_schema(
        &self,
        id: u32,
        rx: Receiver<datafusion::error::Result<SchemaRef>>,
        table: TableInfo,
    ) -> Result<Schema, ConnectorError> {
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

        let mut cols = vec![];
        let fields_list = match table.columns {
            Some(columns) if !columns.is_empty() => {
                for f in c.fields() {
                    if columns.contains(f.name()) {
                        cols.push(f.clone());
                    }
                }
                cols.as_ref()
            }
            _ => c.fields(),
        };

        let fields = map_schema_to_dozer(fields_list).map_err(|e| {
            ConnectorError::DataFusionConnectorError(
                DataFusionConnectorError::DataFusionSchemaError(e),
            )
        })?;

        Ok(Schema {
            identifier: Some(SchemaIdentifier { id, version: 0 }),
            fields,
            primary_index: vec![],
        })
    }
}

pub trait Mapper<T> {
    fn get_schema(
        &self,
        tables: Vec<TableInfo>,
    ) -> Result<Vec<SchemaWithChangesType>, ConnectorError>;
}

impl Mapper<S3Storage> for SchemaMapper<S3Storage> {
    fn get_schema(
        &self,
        tables: Vec<TableInfo>,
    ) -> Result<Vec<SchemaWithChangesType>, ConnectorError> {
        let tables_map: HashMap<String, DataFusionTable> = self
            .config
            .tables
            .clone()
            .into_iter()
            .map(|table| (table.name.clone(), table))
            .collect();
        let details = self.config.details.as_ref().map_or_else(
            || {
                Err(ConnectorError::DataFusionConnectorError(
                    DataFusionConnectorError::DataFusionStorageObjectError(MissingStorageDetails),
                ))
            },
            Ok,
        )?;
        let mut schemas = vec![];

        for (id, table) in tables.iter().enumerate() {
            let data_fusion_table = tables_map.get(&table.table_name).map_or_else(
                || {
                    Err(ConnectorError::DataFusionConnectorError(
                        DataFusionConnectorError::DataFusionStorageObjectError(
                            TableDefinitionNotFound,
                        ),
                    ))
                },
                Ok,
            )?;
            let path = format!(
                "s3://{}/{}/",
                details.bucket_name, data_fusion_table.folder_name
            );

            let table_path = ListingTableUrl::parse(path).map_err(|_| {
                ConnectorError::DataFusionConnectorError(
                    DataFusionConnectorError::DataFusionStorageObjectError(ListingPathParsingError),
                )
            })?;

            let listing_options = map_listing_options(data_fusion_table);

            let (tx, rx) = channel::bounded(1);

            let rt = Runtime::new().map_err(|_| {
                ConnectorError::DataFusionConnectorError(
                    DataFusionConnectorError::RuntimeCreationError,
                )
            })?;

            let details = details.clone();
            let ctx = SessionContext::new();
            let s3 = AmazonS3Builder::new()
                .with_bucket_name(details.bucket_name.to_owned())
                .with_region(details.region.to_owned())
                .with_access_key_id(details.access_key_id.to_owned())
                .with_secret_access_key(details.secret_access_key.to_owned())
                .build()
                .map_or(Err(ConnectorError::InitializationError), Ok)?;

            rt.block_on(async move {
                ctx.runtime_env()
                    .register_object_store("s3", &details.bucket_name, Arc::new(s3));

                let resolved_schema = listing_options
                    .infer_schema(&ctx.state(), &table_path)
                    .await;

                tx.send(resolved_schema)
                    .map_err(|_| {
                        ConnectorError::DataFusionConnectorError(
                            DataFusionConnectorError::InternalError,
                        )
                    })
                    .unwrap();
            });

            let schema = self.map_schema(id as u32, rx, table.clone())?;
            schemas.push((table.table_name.clone(), schema, Nothing))
        }

        Ok(schemas)
    }
}

impl Mapper<LocalStorage> for SchemaMapper<LocalStorage> {
    fn get_schema(
        &self,
        tables: Vec<TableInfo>,
    ) -> Result<Vec<SchemaWithChangesType>, ConnectorError> {
        let tables_map: HashMap<String, DataFusionTable> = self
            .config
            .tables
            .clone()
            .into_iter()
            .map(|table| (table.name.clone(), table))
            .collect();
        let details = self.config.details.as_ref().map_or_else(
            || {
                Err(ConnectorError::DataFusionConnectorError(
                    DataFusionConnectorError::DataFusionStorageObjectError(MissingStorageDetails),
                ))
            },
            Ok,
        )?;

        let mut schemas = vec![];
        for (id, table) in tables.iter().enumerate() {
            let data_fusion_table = tables_map.get(&table.table_name).map_or_else(
                || {
                    Err(ConnectorError::DataFusionConnectorError(
                        DataFusionConnectorError::DataFusionStorageObjectError(
                            TableDefinitionNotFound,
                        ),
                    ))
                },
                Ok,
            )?;
            let path = format!(
                "{}/{}/",
                details.path.clone(),
                data_fusion_table.folder_name
            );

            let listing_options = map_listing_options(data_fusion_table);

            let (tx, rx) = channel::bounded(1);

            let rt = Runtime::new().map_err(|_| {
                ConnectorError::DataFusionConnectorError(
                    DataFusionConnectorError::RuntimeCreationError,
                )
            })?;

            let details = details.clone();
            let ctx = SessionContext::new();
            let ls = LocalFileSystem::new_with_prefix(details.path.clone())
                .map_or(Err(ConnectorError::InitializationError), Ok)?;
            ctx.runtime_env()
                .register_object_store("local", &details.path, Arc::new(ls));

            let table_path = ListingTableUrl::parse(path).map_err(|_| {
                ConnectorError::DataFusionConnectorError(
                    DataFusionConnectorError::DataFusionStorageObjectError(ListingPathParsingError),
                )
            })?;

            rt.block_on(async move {
                let resolved_schema = listing_options
                    .infer_schema(&ctx.state(), &table_path)
                    .await;

                tx.send(resolved_schema)
                    .map_err(|_| {
                        ConnectorError::DataFusionConnectorError(
                            DataFusionConnectorError::InternalError,
                        )
                    })
                    .unwrap()
            });

            let schema = self.map_schema(id as u32, rx, table.clone())?;
            schemas.push((table.table_name.clone(), schema, Nothing))
        }

        Ok(schemas)
    }
}
