use crate::connectors::object_store::table_watcher::FileInfo;
use crate::errors::ObjectStoreObjectError;
use deltalake::datafusion::datasource::file_format::csv::CsvFormat;
use deltalake::datafusion::datasource::file_format::parquet::ParquetFormat;
use deltalake::datafusion::datasource::listing::ListingOptions;
use dozer_types::ingestion_types::Table;
use std::sync::Arc;

pub fn map_listing_options(
    data_fusion_table: &Table,
) -> Result<ListingOptions, ObjectStoreObjectError> {
    if let Some(table) = &data_fusion_table.config {
        match table {
            dozer_types::ingestion_types::TableConfig::CSV(csv) => {
                let format = CsvFormat::default();
                Ok(
                    ListingOptions::new(Arc::new(format))
                        .with_file_extension(csv.extension.clone()),
                )
            }
            dozer_types::ingestion_types::TableConfig::Delta(_) => todo!(),
            dozer_types::ingestion_types::TableConfig::Parquet(parquet) => {
                let format = ParquetFormat::new();
                Ok(ListingOptions::new(Arc::new(format))
                    .with_file_extension(parquet.extension.clone()))
            }
        }
    } else {
        Err(ObjectStoreObjectError::FileFormatUnsupportedError(
            "No file format specified".to_string(),
        ))
    }
}

pub fn is_marker_file_exist(marker_files: Vec<FileInfo>, info: &FileInfo) -> bool {
    for marker_file in marker_files {
        let marker_file_name = match marker_file.name.rsplit_once(".") {
            None => "",
            Some(n) => n.0,
        };
        let file_name = match info.name.rsplit_once(".") {
            None => "",
            Some(n) => n.0,
        };
        if !file_name.is_empty() && marker_file_name == file_name {
            return true;
        }
    }
    return false;
}
