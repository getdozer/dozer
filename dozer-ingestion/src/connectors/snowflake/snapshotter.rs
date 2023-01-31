use crate::connectors::snowflake::connection::client::Client;
use crate::errors::ConnectorError;
use crate::ingestion::Ingestor;
use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Operation, OperationEvent, Record, SchemaIdentifier};

use crate::errors::SnowflakeError::ConnectionError;
use odbc::create_environment_v3;
use std::sync::Arc;

pub struct Snapshotter {}

impl Snapshotter {
    pub fn get_snapshot_table_name(table_name: &String) -> String {
        format!("dozer_{table_name}_snapshot")
    }

    pub fn run(
        client: &Client,
        ingestor: &Arc<RwLock<Ingestor>>,
        table_name: String,
        table_idx: usize,
        offset: usize,
    ) -> Result<(), ConnectorError> {
        let env = create_environment_v3().map_err(|e| e.unwrap()).unwrap();
        let conn = env
            .connect_with_connection_string(&client.get_conn_string())
            .map_err(|e| ConnectionError(Box::new(e)))?;

        let snapshot_table = Snapshotter::get_snapshot_table_name(&table_name);
        let query = format!(
            "CREATE STREAM IF NOT EXISTS {snapshot_table} ON TABLE {table_name} SHOW_INITIAL_ROWS = TRUE;"
        );
        let result = client.exec_stream_creation(&conn, query)?;

        if !result {
            let query = format!(
                "CREATE STREAM IF NOT EXISTS {snapshot_table} ON VIEW {table_name} SHOW_INITIAL_ROWS = TRUE;"
            );
            client.exec(&conn, query)?;
        }

        let mut idx = offset;
        let result = client.fetch(&conn, format!("SELECT * EXCLUDE (\"METADATA$ACTION\", \"METADATA$ISUPDATE\", \"METADATA$ROW_ID\") FROM {snapshot_table};"))?;
        if let Some((_, mut iterator)) = result {
            for values in iterator.by_ref() {
                ingestor
                    .write()
                    .handle_message((
                        (0, idx as u64),
                        IngestionMessage::OperationEvent(OperationEvent {
                            seq_no: 0,
                            operation: Operation::Insert {
                                new: Record {
                                    schema_id: Some(SchemaIdentifier {
                                        id: table_idx as u32,
                                        version: 1,
                                    }),
                                    values,
                                    version: None,
                                },
                            },
                        }),
                    ))
                    .map_err(ConnectorError::IngestorError)?;

                idx += 1;
            }
            iterator.close_cursor()?;
        }

        Ok(())
    }
}
