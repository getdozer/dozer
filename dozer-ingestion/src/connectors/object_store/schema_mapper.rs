use crate::connectors::object_store::adapters::DozerObjectStore;
use crate::connectors::object_store::helper::map_listing_options;
use crate::connectors::object_store::schema_helper::map_schema_to_dozer;
use crate::connectors::{CdcType, ListOrFilterColumns, SourceSchema, SourceSchemaResult};
use crate::errors::ObjectStoreObjectError::ListingPathParsingError;
use crate::errors::{ConnectorError, ObjectStoreConnectorError};
use deltalake::arrow::datatypes::SchemaRef;
use deltalake::datafusion::datasource::listing::ListingTableUrl;
use deltalake::datafusion::prelude::SessionContext;
use dozer_types::log::error;
use dozer_types::types::{Schema, SchemaIdentifier};
use std::sync::Arc;

pub fn map_schema(
    id: u32,
    resolved_schema: SchemaRef,
    table: &ListOrFilterColumns,
) -> Result<Schema, ConnectorError> {
    let fields_list = resolved_schema.fields().iter();

    let fields = match &table.columns {
        Some(columns) if !columns.is_empty() => {
            let fields_list = fields_list.filter(|f| columns.iter().any(|c| c == f.name()));

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

pub async fn get_schema(
    config: &impl DozerObjectStore,
    tables: &[ListOrFilterColumns],
) -> Result<Vec<SourceSchemaResult>, ConnectorError> {
    let mut result = vec![];
    for (id, table) in tables.iter().enumerate() {
        result.push(get_table_schema(config, table, id as u32).await);
    }
    Ok(result)
}

async fn get_table_schema(
    config: &impl DozerObjectStore,
    table: &ListOrFilterColumns,
    id: u32,
) -> SourceSchemaResult {
    let table_name = table.name.clone();

    let params = config.table_params(&table_name)?;

    let table_path = ListingTableUrl::parse(&params.table_path).map_err(|e| {
        ObjectStoreConnectorError::DataFusionStorageObjectError(ListingPathParsingError(
            params.table_path.clone(),
            e,
        ))
    })?;

    let listing_options = map_listing_options(params.data_fusion_table)
        .map_err(ObjectStoreConnectorError::DataFusionStorageObjectError)?;

    let ctx = SessionContext::new();

    ctx.runtime_env().register_object_store(
        params.scheme,
        params.host,
        Arc::new(params.object_store),
    );

    let resolved_schema = listing_options
        .infer_schema(&ctx.state(), &table_path)
        .await
        .map_err(|e| {
            error!("{:?}", e);
            ConnectorError::WrongConnectionConfiguration
        })?;

    let schema = map_schema(id, resolved_schema, table)?;

    Ok(SourceSchema::new(schema, CdcType::Nothing))
}
