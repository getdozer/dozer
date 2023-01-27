use crate::connectors::snowflake::connection::client::Client;
use crate::errors::ConnectorError;
use crate::ingestion::Ingestor;
use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Field, Operation, OperationEvent, Record, SchemaIdentifier};

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
    ) -> Result<(), ConnectorError> {
        let env = create_environment_v3().map_err(|e| e.unwrap()).unwrap();
        let conn = env
            .connect_with_connection_string(&client.get_conn_string())
            .map_err(|e| ConnectionError(Box::new(e)))?;

        let query = format!(
            "CREATE STREAM IF NOT EXISTS {} ON TABLE {} SHOW_INITIAL_ROWS = TRUE;",
            Snapshotter::get_snapshot_table_name(&table_name),
            table_name
        );
        client.exec(&conn, query)?;

        let result = client.fetch(&conn, format!("SELECT * FROM {table_name};"));
        match result {
            Ok(Some((_, mut iterator))) => {
                let mut idx = 0;
                iterator.try_for_each(|values| -> Result<(), ConnectorError> {
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
                                        values: values
                                            .iter()
                                            .map(|v| match v {
                                                None => Field::Null,
                                                Some(s) => s.clone(),
                                            })
                                            .collect(),
                                        version: None,
                                    },
                                },
                            }),
                        ))
                        .map_err(ConnectorError::IngestorError)?;

                    idx += 1;
                    Ok(())
                })?;

                Ok(())
            }
            Err(e) => Err(e),
            Ok(None) => Ok(()),
        }
    }
}
