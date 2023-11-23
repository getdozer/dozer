use datafusion::datasource::listing::ListingTableUrl;
use dozer_ingestion_connector::{
    dozer_types::indicatif::{ProgressBar, ProgressStyle},
    TableIdentifier,
};

use crate::{adapters::DozerObjectStore, ObjectStoreConnectorError};

pub enum Validations {
    Permissions,
}

pub fn validate_connection<T: DozerObjectStore>(
    name: &str,
    tables: Option<&[TableIdentifier]>,
    config: T,
) -> Result<(), ObjectStoreConnectorError> {
    let validations_order: Vec<Validations> = vec![Validations::Permissions];
    let pb = ProgressBar::new(validations_order.len() as u64);
    pb.set_style(
        ProgressStyle::with_template(&format!(
            "[{}] {}",
            name, "{spinner:.green} {wide_msg} {bar}"
        ))
        .unwrap(),
    );
    pb.set_message("Validating connection to source");

    for validation_type in validations_order {
        match validation_type {
            Validations::Permissions => validate_permissions(tables, config.clone())?,
        }

        pb.inc(1);
    }

    pb.finish_and_clear();

    Ok(())
}

fn validate_permissions<T: DozerObjectStore>(
    tables: Option<&[TableIdentifier]>,
    config: T,
) -> Result<(), ObjectStoreConnectorError> {
    if let Some(tables) = tables {
        for table in tables.iter() {
            let params = config.table_params(&table.name)?;
            ListingTableUrl::parse(&params.table_path)
                .map_err(ObjectStoreConnectorError::InternalDataFusionError)?;
        }
    }

    Ok(())
}
