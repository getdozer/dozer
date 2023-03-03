use crate::ingestion::Ingestor;

use super::helper;
use crate::connectors::postgres::connection::helper as connection_helper;
use crate::errors::ConnectorError;
use crate::errors::PostgresConnectorError::{InvalidQueryError, PostgresSchemaError};
use crate::errors::PostgresConnectorError::{SnapshotReadError, SyncWithSnapshotError};
use crossbeam::channel::{unbounded, Sender};

use crate::connectors::postgres::schema::helper::SchemaHelper;
use crate::connectors::TableInfo;
use crate::errors::ConnectorError::PostgresConnectorError;
use dozer_types::types::{Schema, SourceSchema};
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
    pub fn get_tables(&self, tables: Vec<TableInfo>) -> Result<Vec<SourceSchema>, ConnectorError> {
        let helper = SchemaHelper::new(self.conn_config.clone(), None);
        helper
            .get_schemas(Some(tables))
            .map_err(PostgresConnectorError)
    }

    pub fn sync_table(
        schema: Schema,
        name: String,
        conn_config: tokio_postgres::Config,
        sender: Sender<Result<Option<Operation>, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        let mut client_plain =
            connection_helper::connect(conn_config).map_err(PostgresConnectorError)?;

        let column_str: Vec<String> = schema
            .fields
            .iter()
            .map(|f| format!("\"{0}\"", f.name))
            .collect();

        let column_str = column_str.join(",");
        let query = format!("select {column_str} from {name}");
        let stmt = client_plain
            .prepare(&query)
            .map_err(|e| PostgresConnectorError(InvalidQueryError(e)))?;
        let columns = stmt.columns();

        let empty_vec: Vec<String> = Vec::new();
        for msg in client_plain
            .query_raw(&stmt, empty_vec)
            .map_err(|e| PostgresConnectorError(InvalidQueryError(e)))?
            .iterator()
        {
            match msg {
                Ok(msg) => {
                    let evt = helper::map_row_to_operation_event(
                        name.to_string(),
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

    pub fn sync_tables(&self, tables: Vec<TableInfo>) -> Result<(), ConnectorError> {
        let tables = self.get_tables(tables)?;

        let mut left_tables_count = tables.len();

        let (tx, rx) = unbounded();

        for t in tables.iter() {
            let schema = t.schema.clone();
            let name = t.name.clone();
            let conn_config = self.conn_config.clone();
            let sender = tx.clone();
            thread::spawn(move || {
                if let Err(e) = Self::sync_table(schema, name, conn_config, sender.clone()) {
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
                        .handle_message(IngestionMessage::new_op(0, idx, vec![evt]))
                        .map_err(ConnectorError::IngestorError)?;
                    idx += 1;
                }
            }
        }

        Ok(())
    }
}
