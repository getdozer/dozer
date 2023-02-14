use crate::connectors::TableInfo;
use crate::ingestion::Ingestor;

use super::helper;
use super::schema_helper::SchemaHelper;
use crate::connectors::postgres::connection::helper as connection_helper;
use crate::errors::ConnectorError;
use crate::errors::PostgresConnectorError::SyncWithSnapshotError;
use crate::errors::PostgresConnectorError::{InvalidQueryError, PostgresSchemaError};
use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::parking_lot::RwLock;

use crate::errors::ConnectorError::PostgresConnectorError;
use postgres::fallible_iterator::FallibleIterator;
use postgres_types::PgLsn;
use std::cell::RefCell;
use std::sync::Arc;

// 0.4.10
pub struct PostgresSnapshotter {
    pub tables: Option<Vec<TableInfo>>,
    pub conn_config: tokio_postgres::Config,
    pub ingestor: Arc<RwLock<Ingestor>>,
    pub connector_id: u64,
}

impl PostgresSnapshotter {
    pub fn get_tables(
        &self,
        tables: Option<Vec<TableInfo>>,
    ) -> Result<Vec<TableInfo>, ConnectorError> {
        let helper = SchemaHelper::new(self.conn_config.clone(), None);
        let arr = helper.get_tables(tables).unwrap();
        match self.tables.as_ref() {
            None => Ok(arr),
            Some(filtered_tables) => {
                let table_names: Vec<String> = filtered_tables
                    .iter()
                    .map(|t| t.table_name.to_owned())
                    .collect();
                let arr = arr
                    .iter()
                    .filter(|t| table_names.contains(&t.table_name))
                    .cloned()
                    .collect();
                Ok(arr)
            }
        }
    }

    pub fn sync_tables(
        &self,
        tables: Option<Vec<TableInfo>>,
        lsn_option: Option<&(PgLsn, u64)>,
    ) -> Result<Option<Vec<TableInfo>>, ConnectorError> {
        let client_plain = Arc::new(RefCell::new(
            connection_helper::connect(self.conn_config.clone()).map_err(PostgresConnectorError)?,
        ));

        let lsn = lsn_option.map_or(0u64, |(pg_lsn, _)| u64::from(*pg_lsn));
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
                            .write()
                            .handle_message(((lsn, idx), IngestionMessage::OperationEvent(evt)))
                            .map_err(ConnectorError::IngestorError)?;
                    }
                    Err(e) => {
                        return Err(PostgresConnectorError(SyncWithSnapshotError(e.to_string())))
                    }
                }
                idx += 1;
            }
        }

        Ok(Some(tables))
    }
}
