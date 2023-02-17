use crate::connectors::object_store::adapters::DozerObjectStore;
use crate::connectors::object_store::helper::map_listing_options;
use crate::connectors::object_store::schema_helper::map_schema_to_dozer;
use crate::connectors::TableInfo;
use crate::errors::ObjectStoreObjectError::ListingPathParsingError;
use crate::errors::{ConnectorError, ObjectStoreConnectorError};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::prelude::SessionContext;
use dozer_types::log::error;
use dozer_types::types::ReplicationChangesTrackingType::Nothing;
use dozer_types::types::{Schema, SchemaIdentifier, SchemaWithChangesType};
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
        resolved_schema: SchemaRef,
        table: &TableInfo,
    ) -> Result<Schema, ConnectorError> {
        let fields_list = resolved_schema.fields().iter();

        let fields = match &table.columns {
            Some(columns) if !columns.is_empty() => {
                let fields_list =
                    fields_list.filter(|f| columns.iter().any(|c| &c.name == f.name()));

                map_schema_to_dozer(fields_list)
            }
            _ => map_schema_to_dozer(fields_list),
        };

        Ok(Schema {
            identifier: Some(SchemaIdentifier { id, version: 0 }),
            fields: fields.map_err(ObjectStoreConnectorError::DataFusionSchemaError)?,
            primary_index: vec![],
        })
    }
}

pub trait Mapper<T> {
    fn get_schema(
        &self,
        tables: Option<Vec<TableInfo>>,
    ) -> Result<Vec<SchemaWithChangesType>, ConnectorError>;
}

impl<T: DozerObjectStore> Mapper<T> for SchemaMapper<T> {
    fn get_schema(
        &self,
        tables: Option<Vec<TableInfo>>,
    ) -> Result<Vec<SchemaWithChangesType>, ConnectorError> {
        let rt = Runtime::new().map_err(|_| ObjectStoreConnectorError::RuntimeCreationError)?;

        let tables_list = tables.unwrap_or_else(|| {
            self.config
                .tables()
                .iter()
                .map(|t| TableInfo {
                    name: t.name.clone(),
                    table_name: t.name.clone(),
                    id: 0,
                    columns: None,
                })
                .collect()
        });

        tables_list
            .iter()
            .enumerate()
            .map(|(id, table)| {
                let table_name = table.table_name.clone();

                let params = self.config.table_params(&table_name)?;

                let table_path = ListingTableUrl::parse(params.table_path).map_err(|_| {
                    ObjectStoreConnectorError::DataFusionStorageObjectError(ListingPathParsingError)
                })?;

                let listing_options = map_listing_options(params.data_fusion_table);

                let ctx = SessionContext::new();

                ctx.runtime_env().register_object_store(
                    params.scheme,
                    params.host,
                    Arc::new(params.object_store),
                );

                let resolved_schema = rt
                    .block_on(listing_options.infer_schema(&ctx.state(), &table_path))
                    .map_err(|e| {
                        error!("{:?}", e);
                        ConnectorError::WrongConnectionConfiguration
                    })?;

                let schema = self.map_schema(id as u32, resolved_schema, table)?;

                Ok((table_name, schema, Nothing))
            })
            .collect()
    }
}
