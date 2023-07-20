use crate::connectors::object_store::adapters::DozerObjectStore;
use crate::connectors::object_store::schema_helper::map_schema_to_dozer;
use crate::connectors::{CdcType, ListOrFilterColumns, SourceSchema, SourceSchemaResult};
use crate::errors::ObjectStoreObjectError::ListingPathParsingError;
use crate::errors::{ConnectorError, ObjectStoreConnectorError};
use deltalake::arrow::datatypes::SchemaRef;
use deltalake::datafusion::datasource::file_format::csv::CsvFormat;
use deltalake::datafusion::datasource::file_format::parquet::ParquetFormat;
use deltalake::datafusion::datasource::listing::{ListingOptions, ListingTableUrl};
use deltalake::datafusion::prelude::SessionContext;
use deltalake::s3_storage_options;
use dozer_types::log::error;
use dozer_types::types::Schema;
use std::collections::HashMap;
use std::sync::Arc;

pub fn map_schema(
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
        fields: fields.map_err(ObjectStoreConnectorError::DataFusionSchemaError)?,
        primary_index: vec![],
    })
}

pub async fn get_schema(
    config: &impl DozerObjectStore,
    tables: &[ListOrFilterColumns],
) -> Result<Vec<SourceSchemaResult>, ConnectorError> {
    let mut result = vec![];
    for table in tables.iter() {
        result.push(get_table_schema(config, table).await);
    }
    Ok(result)
}

async fn get_table_schema(
    config: &impl DozerObjectStore,
    table: &ListOrFilterColumns,
) -> SourceSchemaResult {
    let params = &config.table_params(&table.name)?;

    if let Some(table_config) = &params.data_fusion_table.config {
        match table_config {
            dozer_types::ingestion_types::TableConfig::CSV(table_config) => {
                let format = CsvFormat::default();
                let listing_options = ListingOptions::new(Arc::new(format))
                    .with_file_extension(table_config.extension.clone());
                get_object_schema(table, config, listing_options).await
            }
            dozer_types::ingestion_types::TableConfig::Delta(_table_config) => {
                get_delta_schema(table, config).await
            }
            dozer_types::ingestion_types::TableConfig::Parquet(table_config) => {
                let format = ParquetFormat::default();
                let listing_options = ListingOptions::new(Arc::new(format))
                    .with_file_extension(table_config.extension.clone());

                get_object_schema(table, config, listing_options).await
            }
        }
    } else {
        Err(ConnectorError::UnavailableConnectionConfiguration(
            "Unable to get the table schema".to_string(),
        ))
    }
}

async fn get_object_schema(
    table: &ListOrFilterColumns,
    store_config: &impl DozerObjectStore,
    listing_options: ListingOptions,
) -> SourceSchemaResult {
    let params = store_config.table_params(&table.name)?;

    let table_path = ListingTableUrl::parse(&params.table_path).map_err(|e| {
        ObjectStoreConnectorError::DataFusionStorageObjectError(ListingPathParsingError(
            params.table_path.clone(),
            e,
        ))
    })?;

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
            ConnectorError::UnableToInferSchema(e)
        })?;

    let schema = map_schema(resolved_schema, table)?;

    Ok(SourceSchema::new(schema, CdcType::Nothing))
}

async fn get_delta_schema(
    table: &ListOrFilterColumns,
    store_config: &impl DozerObjectStore,
) -> SourceSchemaResult {
    let params = store_config.table_params(&table.name)?;

    let table_path = params.table_path;

    let ctx = SessionContext::new();
    //let delta_table = deltalake::open_table(table_path).await?;

    ctx.runtime_env().register_object_store(
        params.scheme,
        params.host,
        Arc::new(params.object_store),
    );

    let delta_table = if params.aws_region.is_none() {
        deltalake::open_table(table_path).await.unwrap()
    } else {
        let storage_options = HashMap::from([
            (
                s3_storage_options::AWS_REGION.to_string(),
                params.aws_region.clone().unwrap(),
            ),
            (
                s3_storage_options::AWS_ACCESS_KEY_ID.to_string(),
                params.aws_access_key_id.clone().unwrap(),
            ),
            (
                s3_storage_options::AWS_SECRET_ACCESS_KEY.to_string(),
                params.aws_secret_access_key.clone().unwrap(),
            ),
        ]);

        deltalake::open_table_with_storage_options(&table_path, storage_options)
            .await
            .unwrap()
    };

    let arrow_schema: SchemaRef = (*ctx.read_table(Arc::new(delta_table))?.schema())
        .clone()
        .into();
    let schema = map_schema(arrow_schema, table)?;
    Ok(SourceSchema::new(schema, CdcType::Nothing))
}
