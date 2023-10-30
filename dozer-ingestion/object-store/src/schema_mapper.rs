use deltalake::arrow::datatypes::SchemaRef;
use deltalake::datafusion::datasource::file_format::csv::CsvFormat;
use deltalake::datafusion::datasource::file_format::parquet::ParquetFormat;
use deltalake::datafusion::datasource::listing::{ListingOptions, ListingTableUrl};
use deltalake::datafusion::prelude::SessionContext;
use dozer_ingestion_connector::dozer_types::log::error;
use dozer_ingestion_connector::dozer_types::models::ingestion_types::TableConfig;
use dozer_ingestion_connector::dozer_types::types::Schema;
use dozer_ingestion_connector::utils::ListOrFilterColumns;
use dozer_ingestion_connector::{CdcType, SourceSchema, SourceSchemaResult};
use std::sync::Arc;

use crate::adapters::DozerObjectStore;
use crate::schema_helper::map_schema_to_dozer;
use crate::{ObjectStoreConnectorError, ObjectStoreObjectError, ObjectStoreSchemaError};

pub fn map_schema(
    resolved_schema: SchemaRef,
    table: &ListOrFilterColumns,
) -> Result<Schema, ObjectStoreSchemaError> {
    let fields_list = resolved_schema.fields().iter();

    let fields = match &table.columns {
        Some(columns) if !columns.is_empty() => {
            let fields_list = fields_list.filter(|f| columns.iter().any(|c| c == f.name()));

            map_schema_to_dozer(fields_list)
        }
        _ => map_schema_to_dozer(fields_list),
    };

    Ok(Schema {
        fields: fields?,
        primary_index: vec![],
    })
}

pub async fn get_schema(
    config: &impl DozerObjectStore,
    tables: &[ListOrFilterColumns],
) -> Vec<SourceSchemaResult> {
    let mut result = vec![];
    for table in tables.iter() {
        result.push(get_table_schema(config, table).await);
    }
    result
}

async fn get_table_schema(
    config: &impl DozerObjectStore,
    table: &ListOrFilterColumns,
) -> SourceSchemaResult {
    let params = &config.table_params(&table.name)?;

    match &params.data_fusion_table.config {
        TableConfig::CSV(table_config) => {
            let format = CsvFormat::default();
            let listing_options = ListingOptions::new(Arc::new(format))
                .with_file_extension(table_config.extension.clone());
            get_object_schema(table, config, listing_options).await
        }
        TableConfig::Parquet(table_config) => {
            let format = ParquetFormat::default();
            let listing_options = ListingOptions::new(Arc::new(format))
                .with_file_extension(table_config.extension.clone());

            get_object_schema(table, config, listing_options).await
        }
    }
}

async fn get_object_schema(
    table: &ListOrFilterColumns,
    store_config: &impl DozerObjectStore,
    listing_options: ListingOptions,
) -> SourceSchemaResult {
    let params = store_config.table_params(&table.name)?;

    let table_path = ListingTableUrl::parse(&params.table_path).map_err(|e| {
        ObjectStoreConnectorError::DataFusionStorageObjectError(
            ObjectStoreObjectError::ListingPathParsingError(params.table_path.clone(), e),
        )
    })?;

    let ctx = SessionContext::new();

    ctx.runtime_env()
        .register_object_store(&params.url, Arc::new(params.object_store));

    let resolved_schema = listing_options
        .infer_schema(&ctx.state(), &table_path)
        .await
        .map_err(|e| {
            error!("{:?}", e);
            ObjectStoreConnectorError::InternalDataFusionError(e)
        })?;

    let schema = map_schema(resolved_schema, table)?;

    Ok(SourceSchema::new(schema, CdcType::Nothing))
}
