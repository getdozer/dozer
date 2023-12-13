use datafusion::datasource::{
    file_format::{csv::CsvFormat, parquet::ParquetFormat},
    listing::ListingOptions,
};
use dozer_ingestion_connector::dozer_types::models::ingestion_types::{Table, TableConfig};
use std::sync::Arc;

use crate::{table_watcher::FileInfo, ObjectStoreObjectError};

pub fn map_listing_options(
    data_fusion_table: &Table,
) -> Result<ListingOptions, ObjectStoreObjectError> {
    match &data_fusion_table.config {
        TableConfig::CSV(csv) => {
            let format = CsvFormat::default();
            Ok(ListingOptions::new(Arc::new(format)).with_file_extension(csv.extension.clone()))
        }
        TableConfig::Parquet(parquet) => {
            let format = ParquetFormat::new();
            Ok(
                ListingOptions::new(Arc::new(format))
                    .with_file_extension(parquet.extension.clone()),
            )
        }
    }
}

pub fn is_marker_file_exist(marker_files: Vec<FileInfo>, info: &FileInfo) -> bool {
    for marker_file in marker_files {
        let marker_file_name = match marker_file.name.rsplit_once('.') {
            None => "",
            Some(n) => n.0,
        };
        let file_name = match info.name.rsplit_once('.') {
            None => "",
            Some(n) => n.0,
        };
        if !file_name.is_empty() && marker_file_name == file_name {
            return true;
        }
    }
    false
}
