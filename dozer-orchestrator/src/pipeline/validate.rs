use std::collections::HashMap;

use dozer_ingestion::{
    connectors::{
        get_connector, get_connector_info_table, ColumnInfo, TableInfo, ValidationResults,
    },
    errors::ConnectorError,
};
use dozer_types::{
    log::{error, info},
    models::{connection::Connection, source::Source},
};

use crate::{console_helper::get_colored_text, errors::OrchestrationError};

pub fn validate_grouped_connections(
    grouped_connections: &HashMap<&str, Vec<&Source>>,
) -> Result<(), OrchestrationError> {
    for sources_group in grouped_connections.values() {
        let first_source = sources_group.get(0).unwrap();

        if let Some(connection) = &first_source.connection {
            let tables: Vec<TableInfo> = sources_group
                .iter()
                .map(|source| TableInfo {
                    name: source.table_name.clone(),
                    schema: source.schema.clone(),
                    columns: Some(
                        source
                            .columns
                            .iter()
                            .map(|c| ColumnInfo {
                                name: c.clone(),
                                data_type: None,
                            })
                            .collect(),
                    ),
                })
                .collect();

            if let Some(info_table) = get_connector_info_table(connection) {
                info!("[{}] Connection parameters", connection.name);
                info_table.printstd();
            }

            validate(connection.clone(), Some(tables.clone()))
                .map_err(|e| {
                    error!(
                        "[{}] {} Connection validation error: {}",
                        connection.name,
                        get_colored_text("X", "31"),
                        e
                    );
                    OrchestrationError::SourceValidationError
                })
                .map(|_| {
                    info!(
                        "[{}] {} Connection validation completed",
                        connection.name,
                        get_colored_text("✓", "32")
                    );
                })?;

            validate_schema(connection.clone(), &tables).map_or_else(
                |e| {
                    error!(
                        "[{}] {} Schema validation error: {}",
                        connection.name,
                        get_colored_text("X", "31"),
                        e
                    );
                    Err(OrchestrationError::SourceValidationError)
                },
                |r| {
                    let mut all_valid = true;
                    for (table_name, validation_result) in r.into_iter() {
                        let is_valid = validation_result.iter().all(|(_, result)| result.is_ok());

                        if is_valid {
                            info!(
                                "[{}][{}] {} Schema validation completed",
                                connection.name,
                                table_name,
                                get_colored_text("✓", "32")
                            );
                        } else {
                            all_valid = false;
                            for (_, error) in validation_result {
                                if let Err(e) = error {
                                    error!(
                                        "[{}][{}] {} Schema validation error: {}",
                                        connection.name,
                                        table_name,
                                        get_colored_text("X", "31"),
                                        e
                                    );
                                }
                            }
                        }
                    }

                    if !all_valid {
                        return Err(OrchestrationError::SourceValidationError);
                    }

                    Ok(())
                },
            )?;
        }
    }

    Ok(())
}

pub fn validate(input: Connection, tables: Option<Vec<TableInfo>>) -> Result<(), ConnectorError> {
    get_connector(input, tables.clone())?.validate(tables)
}

pub fn validate_schema(
    input: Connection,
    tables: &[TableInfo],
) -> Result<ValidationResults, ConnectorError> {
    get_connector(input, Some(tables.to_vec()))?.validate_schemas(tables)
}
