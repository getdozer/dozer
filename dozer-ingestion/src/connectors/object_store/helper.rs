use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use dozer_types::ingestion_types::Table;
use std::sync::Arc;

pub fn map_listing_options(data_fusion_table: &Table) -> ListingOptions {
    match data_fusion_table.file_type.as_str() {
        "parquet" => {
            let format = ParquetFormat::new();
            ListingOptions::new(Arc::new(format))
                .with_file_extension(data_fusion_table.extension.clone())
        }
        "csv" => {
            let format = CsvFormat::default();
            ListingOptions::new(Arc::new(format))
                .with_file_extension(data_fusion_table.extension.clone())
        }
        _ => {
            let format = ParquetFormat::new();
            ListingOptions::new(Arc::new(format))
                .with_file_extension(data_fusion_table.extension.clone())
        }
    }
}
