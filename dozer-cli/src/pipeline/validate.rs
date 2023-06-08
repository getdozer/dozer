use std::collections::HashMap;

use dozer_ingestion::connectors::{
    get_connector, get_connector_info_table, TableIdentifier, TableInfo,
};
use dozer_types::{
    log::{error, info},
    models::source::Source,
};

use crate::console_helper::get_colored_text;
use crate::console_helper::GREEN;
use crate::console_helper::RED;
use crate::errors::OrchestrationError;

pub async fn validate_grouped_connections(
    grouped_connections: &HashMap<&str, Vec<&Source>>,
) -> Result<(), OrchestrationError> {
    for sources_group in grouped_connections.values() {
        let first_source = sources_group.get(0).unwrap();

        if let Some(connection) = &first_source.connection {
            let connector = get_connector(connection.clone())?;

            if let Some(info_table) = get_connector_info_table(connection) {
                info!(
                    "[{}] Connection parameters\n{}",
                    connection.name, info_table
                );
            }

            let tables = sources_group
                .iter()
                .map(|source| {
                    TableIdentifier::new(source.schema.clone(), source.table_name.clone())
                })
                .collect::<Vec<_>>();
            connector
                .validate_tables(&tables)
                .await
                .map_err(|e| {
                    error!(
                        "[{}] {} Connection validation error: {}",
                        connection.name,
                        get_colored_text("X", RED),
                        e
                    );
                    OrchestrationError::SourceValidationError
                })
                .map(|_| {
                    info!(
                        "[{}] {} Connection validation completed",
                        connection.name,
                        get_colored_text("✓", GREEN)
                    );
                })?;

            let tables = sources_group
                .iter()
                .map(|source| TableInfo {
                    schema: source.schema.clone(),
                    name: source.table_name.clone(),
                    column_names: source.columns.clone(),
                })
                .collect::<Vec<_>>();
            connector.get_schemas(&tables).await.map_or_else(
                |e| {
                    error!(
                        "[{}] {} Schema validation error: {}",
                        connection.name,
                        get_colored_text("X", RED),
                        e
                    );
                    Err(OrchestrationError::SourceValidationError)
                },
                |schema_results| {
                    let mut all_valid = true;
                    for (schema_result, table) in schema_results.into_iter().zip(tables) {
                        match schema_result {
                            Ok(_) => {
                                info!(
                                    "[{}][{}] {} Schema validation completed",
                                    connection.name,
                                    table.name,
                                    get_colored_text("✓", GREEN)
                                );
                            }
                            Err(e) => {
                                all_valid = false;
                                error!(
                                    "[{}][{}] {} Schema validation error: {}",
                                    connection.name,
                                    table.name,
                                    get_colored_text("X", RED),
                                    e
                                );
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
