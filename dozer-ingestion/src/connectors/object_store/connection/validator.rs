use crate::connectors::object_store::adapters::DozerObjectStore;
use crate::connectors::TableIdentifier;
use crate::errors::{ConnectorError, ObjectStoreConnectorError, ObjectStoreTableReaderError};
use deltalake::datafusion::datasource::listing::ListingTableUrl;

use dozer_types::indicatif::ProgressStyle;

pub enum Validations {
    Permissions,
}

pub fn validate_connection<T: DozerObjectStore>(
    name: &str,
    tables: Option<&[TableIdentifier]>,
    config: T,
) -> Result<(), ConnectorError> {
    let validations_order: Vec<Validations> = vec![Validations::Permissions];
    let pb = dozer_types::indicatif::ProgressBar::new(validations_order.len() as u64);
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
) -> Result<(), ConnectorError> {
    if let Some(tables) = tables {
        for table in tables.iter() {
            let params = config.table_params(&table.name)?;
            ListingTableUrl::parse(&params.table_path).map_err(|e| {
                ConnectorError::ObjectStoreConnectorError(
                    ObjectStoreConnectorError::TableReaderError(
                        ObjectStoreTableReaderError::TableReadFailed(e),
                    ),
                )
            })?;
        }
    }

    Ok(())
}
