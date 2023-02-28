use crate::connectors::TableInfo;
use crate::ingestion::Ingestor;

use super::helper;
use crate::connectors::postgres::connection::helper as connection_helper;
use crate::connectors::postgres::schema::helper::SchemaHelper;
use crate::errors::ConnectorError;
use crate::errors::PostgresConnectorError::{InvalidQueryError, PostgresSchemaError};
use crate::errors::PostgresConnectorError::{SnapshotReadError, SyncWithSnapshotError};
use crossbeam::channel::{unbounded, Sender};

use crate::errors::ConnectorError::PostgresConnectorError;
use postgres::fallible_iterator::FallibleIterator;

use std::thread;

use dozer_types::ingestion_types::IngestionMessage;

use dozer_types::types::Operation;

pub struct PostgresSnapshotter<'a> {
    pub tables: Vec<TableInfo>,
    pub conn_config: tokio_postgres::Config,
    pub ingestor: &'a Ingestor,
    pub connector_id: u64,
}

impl<'a> PostgresSnapshotter<'a> {
    pub fn get_tables(&self, tables: Vec<TableInfo>) -> Result<Vec<TableInfo>, ConnectorError> {
        let table_names: Vec<String> = tables.iter().map(|t| t.table_name.to_owned()).collect();

        let helper = SchemaHelper::new(self.conn_config.clone(), None);
        Ok(helper
            .get_tables(Some(tables.as_slice()))?
            .iter()
            .filter(|t| table_names.contains(&t.table_name))
            .cloned()
            .collect())
    }

    pub fn sync_table(
        table_info: TableInfo,
        conn_config: tokio_postgres::Config,
        sender: Sender<Result<Option<Operation>, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        let mut client_plain = connection_helper::connect(conn_config).map_err(PostgresConnectorError)?;

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
            .prepare(&query)
            .map_err(|e| PostgresConnectorError(InvalidQueryError(e)))?;
        let columns = stmt.columns();

        // Ingest schema for every table
        let schema = helper::map_schema(&table_info.id, columns)?;

        let empty_vec: Vec<String> = Vec::new();
        for msg in client_plain
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

                    sender.send(Ok(Some(evt))).unwrap();
                }
                Err(e) => return Err(PostgresConnectorError(SyncWithSnapshotError(e.to_string()))),
            }
        }

        // After table read is finished, send None as message to inform receiver loop about end of table
        sender.send(Ok(None)).unwrap();
        Ok(())
    }

    pub fn sync_tables(&self, tables: Vec<TableInfo>) -> Result<Vec<TableInfo>, ConnectorError> {
        let tables = self.get_tables(tables)?;

        let mut left_tables_count = tables.len();

        let (tx, rx) = unbounded();

        for t in tables.iter() {
            let table_info = t.clone();
            let conn_config = self.conn_config.clone();
            let sender = tx.clone();
            thread::spawn(move || {
                if let Err(e) = Self::sync_table(table_info, conn_config, sender.clone()) {
                    sender.send(Err(e)).unwrap();
                }
            });
        }

        let mut idx = 0;
        loop {
            let message = rx
                .recv()
                .map_err(|_| PostgresConnectorError(SnapshotReadError))??;
            match message {
                None => {
                    left_tables_count -= 1;
                    if left_tables_count == 0 {
                        break;
                    }
                }
                Some(evt) => {
                    self.ingestor
                        .handle_message(((0, idx), IngestionMessage::OperationEvent(evt)))
                        .map_err(ConnectorError::IngestorError)?;
                    idx += 1;
                }
            }
        }

        Ok(tables)
    }
}
