use crate::connectors::TableInfo;
use crate::ingestion::Ingestor;

use super::helper;
use super::schema_helper::SchemaHelper;
use crate::errors::ConnectorError;
use crate::errors::PostgresConnectorError::PostgresSchemaError;
use crate::errors::PostgresConnectorError::SyncWithSnapshotError;
use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::parking_lot::RwLock;
use dozer_types::types::Commit;
use postgres::fallible_iterator::FallibleIterator;
use postgres::Error;
use postgres::{Client, NoTls};
use std::cell::RefCell;
use std::sync::Arc;

// 0.4.10
pub struct PostgresSnapshotter {
    pub tables: Option<Vec<TableInfo>>,
    pub conn_str: String,
    pub ingestor: Arc<RwLock<Ingestor>>,
    pub connector_id: u64,
}

impl PostgresSnapshotter {
    fn _connect(&mut self) -> Result<Client, Error> {
        let client = Client::connect(&self.conn_str, NoTls)?;
        Ok(client)
    }

    pub fn get_tables(&self) -> Result<Vec<TableInfo>, ConnectorError> {
        match self.tables.as_ref() {
            None => {
                let mut helper = SchemaHelper {
                    conn_str: self.conn_str.clone(),
                };
                let arr = helper.get_tables(None)?;
                Ok(arr)
            }
            Some(arr) => Ok(arr.clone()),
        }
    }

    pub fn sync_tables(&self) -> Result<Vec<String>, ConnectorError> {
        let client_plain = Arc::new(RefCell::new(helper::connect(self.conn_str.clone())?));

        let tables = self.get_tables()?;

        let mut idx: u32 = 0;
        for table_info in tables.iter() {
            let column_str: Vec<String> = table_info
                .columns
                .clone()
                .map_or(Err(ConnectorError::ColumnsNotFound), Ok)?
                .iter()
                .map(|c| format!("\"{}\"", c))
                .collect();

            let column_str = column_str.join(",");
            let query = format!("select {} from {}", column_str, table_info.name);
            let stmt = client_plain
                .clone()
                .borrow_mut()
                .prepare(&query)
                .map_err(|_| ConnectorError::InvalidQueryError)?;
            let columns = stmt.columns();

            // Ingest schema for every table
            let schema = helper::map_schema(&table_info.id, columns)?;

            self.ingestor
                .write()
                .handle_message((self.connector_id, IngestionMessage::Schema(schema.clone())))
                .map_err(ConnectorError::IngestorError)?;

            let empty_vec: Vec<String> = Vec::new();
            for msg in client_plain
                .clone()
                .borrow_mut()
                .query_raw(&stmt, empty_vec)
                .map_err(|_| ConnectorError::InvalidQueryError)?
                .iterator()
            {
                match msg {
                    Ok(msg) => {
                        let evt = helper::map_row_to_operation_event(
                            table_info.name.to_string(),
                            schema
                                .identifier
                                .clone()
                                .map_or(Err(ConnectorError::SchemaIdentifierNotFound), Ok)?,
                            &msg,
                            columns,
                            idx,
                        )
                        .map_err(|e| {
                            ConnectorError::PostgresConnectorError(PostgresSchemaError(e))
                        })?;

                        self.ingestor
                            .write()
                            .handle_message((
                                self.connector_id,
                                IngestionMessage::OperationEvent(evt),
                            ))
                            .map_err(ConnectorError::IngestorError)?;
                    }
                    Err(e) => {
                        return Err(ConnectorError::PostgresConnectorError(
                            SyncWithSnapshotError(e.to_string()),
                        ))
                    }
                }
                idx += 1;
            }

            self.ingestor
                .write()
                .handle_message((
                    self.connector_id,
                    IngestionMessage::Commit(Commit { seq_no: 0, lsn: 0 }),
                ))
                .map_err(ConnectorError::IngestorError)?;
        }

        let table_names = tables.iter().map(|t| t.name.clone()).collect();

        Ok(table_names)
    }
}
