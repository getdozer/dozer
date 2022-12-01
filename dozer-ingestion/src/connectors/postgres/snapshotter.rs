use crate::connectors::TableInfo;
use crate::ingestion::Ingestor;

use super::helper;
use super::schema_helper::SchemaHelper;
use crate::connectors::postgres::connection::helper as connection_helper;
use crate::errors::ConnectorError;
use crate::errors::PostgresConnectorError::PostgresSchemaError;
use crate::errors::PostgresConnectorError::SyncWithSnapshotError;
use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::parking_lot::RwLock;
use dozer_types::types::Commit;
use postgres::fallible_iterator::FallibleIterator;
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
                let table_names: Vec<String> =
                    filtered_tables.iter().map(|t| t.name.to_owned()).collect();
                let arr = arr
                    .iter()
                    .filter(|t| table_names.contains(&t.name))
                    .cloned()
                    .collect();
                Ok(arr)
            }
        }
    }

    pub fn sync_tables(
        &self,
        tables: Option<Vec<TableInfo>>,
    ) -> Result<Option<Vec<TableInfo>>, ConnectorError> {
        let client_plain = Arc::new(RefCell::new(connection_helper::connect(
            self.conn_config.clone(),
        )?));

        let tables = self.get_tables(tables)?;

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
                .handle_message((
                    self.connector_id,
                    IngestionMessage::Schema(table_info.name.clone(), schema.clone()),
                ))
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

        Ok(Some(tables))
    }
}
