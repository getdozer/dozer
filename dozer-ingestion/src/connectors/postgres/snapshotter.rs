use crate::connectors::TableInfo;
use crate::ingestion::Ingestor;

use super::helper;
use super::schema_helper::SchemaHelper;
use crate::connectors::postgres::connection::helper as connection_helper;
use crate::errors::ConnectorError;
use crate::errors::PostgresConnectorError::SyncWithSnapshotError;
use crate::errors::PostgresConnectorError::{InvalidQueryError, PostgresSchemaError};
use dozer_types::ingestion_types::IngestionMessage;

use crate::errors::ConnectorError::PostgresConnectorError;
use postgres::fallible_iterator::FallibleIterator;
use std::cell::RefCell;
use std::sync::Arc;

pub struct PostgresSnapshotter<'a> {
    pub tables: Vec<TableInfo>,
    pub conn_config: tokio_postgres::Config,
    pub ingestor: &'a Ingestor,
    pub connector_id: u64,
}

impl<'a> PostgresSnapshotter<'a> {
    pub fn get_tables(
        &self,
        tables: Vec<TableInfo>
    ) -> Result<Vec<TableInfo>, ConnectorError> {
        let table_names: Vec<String> = tables
            .iter()
            .map(|t| t.table_name.to_owned())
            .collect();

        let helper = SchemaHelper::new(self.conn_config.clone(), None);
        Ok(helper.get_tables(Some(tables.as_slice()))?
            .iter()
            .filter(|t| table_names.contains(&t.table_name))
            .cloned()
            .collect())
    }

    pub fn sync_tables(
        &self,
        tables: Vec<TableInfo>,
    ) -> Result<Vec<TableInfo>, ConnectorError> {
        let client_plain = Arc::new(RefCell::new(
            connection_helper::connect(self.conn_config.clone()).map_err(PostgresConnectorError)?,
        ));

        let tables = self.get_tables(tables)?;

        let mut idx: u64 = 0;
        for table_info in tables.iter() {
            let column_str: Vec<String> = table_info
                .columns
                .clone()
                .map_or(Err(ConnectorError::ColumnsNotFound), Ok)?
                .iter()
                .map(|c| format!("\"{0}\"", c.name))
                .collect();

            let column_str = column_str.join(",");
            let query = format!("select {} from {}", column_str, table_info.table_name);
            let stmt = client_plain
                .clone()
                .borrow_mut()
                .prepare(&query)
                .map_err(|e| PostgresConnectorError(InvalidQueryError(e)))?;
            let columns = stmt.columns();

            // Ingest schema for every table
            let schema = helper::map_schema(&table_info.id, columns)?;

            let empty_vec: Vec<String> = Vec::new();
            for msg in client_plain
                .clone()
                .borrow_mut()
                .query_raw(&stmt, empty_vec)
                .map_err(|e| PostgresConnectorError(InvalidQueryError(e)))?
                .iterator()
            {
                match msg {
                    Ok(msg) => {
                        let evt = helper::map_row_to_operation_event(
                            table_info.table_name.to_string(),
                            schema
                                .identifier
                                .map_or(Err(ConnectorError::SchemaIdentifierNotFound), Ok)?,
                            &msg,
                            columns,
                        )
                        .map_err(|e| PostgresConnectorError(PostgresSchemaError(e)))?;

                        self.ingestor
                            .handle_message(((0, idx), IngestionMessage::OperationEvent(evt)))
                            .map_err(ConnectorError::IngestorError)?;
                    }
                    Err(e) => {
                        return Err(PostgresConnectorError(SyncWithSnapshotError(e.to_string())))
                    }
                }
                idx += 1;
            }
        }

        Ok(tables)
    }
}
