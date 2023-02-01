use crate::connectors::snowflake::connection::client::Client;

use crate::errors::{ConnectorError, SnowflakeError};
use crate::ingestion::Ingestor;
use dozer_types::ingestion_types::IngestionMessage;

use dozer_types::parking_lot::RwLock;

use crate::errors::SnowflakeStreamError::{CannotDetermineAction, UnsupportedActionInStream};
use dozer_types::types::{Field, Operation, OperationEvent, Record, SchemaIdentifier};
use odbc::create_environment_v3;
use std::sync::Arc;

#[derive(Default)]
pub struct StreamConsumer {}

impl StreamConsumer {
    pub fn new() -> Self {
        Self {}
    }

    pub fn get_stream_table_name(table_name: &str) -> String {
        format!("dozer_{table_name}_stream")
    }

    pub fn get_stream_temp_table_name(table_name: &str) -> String {
        format!("dozer_{table_name}_stream_temp")
    }

    pub fn is_stream_created(client: &Client, table_name: String) -> Result<bool, ConnectorError> {
        let env = create_environment_v3().map_err(|e| e.unwrap()).unwrap();
        let conn = env
            .connect_with_connection_string(&client.get_conn_string())
            .unwrap();

        client
            .stream_exist(&conn, &Self::get_stream_table_name(&table_name))
            .map_err(ConnectorError::SnowflakeError)
    }

    pub fn create_stream(client: &Client, table_name: &String) -> Result<(), ConnectorError> {
        let env = create_environment_v3().map_err(|e| e.unwrap()).unwrap();
        let conn = env
            .connect_with_connection_string(&client.get_conn_string())
            .unwrap();

        let query = format!(
            "CREATE STREAM {} on table {} SHOW_INITIAL_ROWS = TRUE",
            Self::get_stream_table_name(table_name),
            table_name,
        );

        let result = client.exec_stream_creation(&conn, query)?;

        if !result {
            let query = format!(
                "CREATE STREAM {} on view {} SHOW_INITIAL_ROWS = TRUE",
                Self::get_stream_table_name(table_name),
                table_name
            );
            client.exec(&conn, query)?;
        }

        Ok(())
    }

    fn map_record(row: Vec<Field>, table_idx: usize) -> Record {
        Record {
            schema_id: Some(SchemaIdentifier {
                id: table_idx as u32,
                version: 1,
            }),
            values: row,
            version: None,
        }
    }

    fn get_ingestion_message(
        row: Vec<Field>,
        action_idx: usize,
        used_columns_for_schema: usize,
        table_idx: usize,
    ) -> Result<IngestionMessage, ConnectorError> {
        if let Field::String(action) = row.get(action_idx).unwrap() {
            let mut row_mut = row.clone();
            let insert_action = &"INSERT";
            let delete_action = &"DELETE";

            if insert_action == action {
                row_mut.truncate(used_columns_for_schema);
                Ok(IngestionMessage::OperationEvent(OperationEvent {
                    seq_no: 0,
                    operation: Operation::Insert {
                        new: Self::map_record(row, table_idx),
                    },
                }))
            } else if delete_action == action {
                row_mut.truncate(used_columns_for_schema);

                Ok(IngestionMessage::OperationEvent(OperationEvent {
                    seq_no: 0,
                    operation: Operation::Delete {
                        old: Self::map_record(row, table_idx),
                    },
                }))
            } else {
                Err(ConnectorError::SnowflakeError(
                    SnowflakeError::SnowflakeStreamError(UnsupportedActionInStream(action.clone())),
                ))
            }
        } else {
            Err(ConnectorError::SnowflakeError(
                SnowflakeError::SnowflakeStreamError(CannotDetermineAction),
            ))
        }
    }

    pub fn consume_stream(
        &mut self,
        client: &Client,
        table_name: &str,
        ingestor: &Arc<RwLock<Ingestor>>,
        table_idx: usize,
    ) -> Result<(), ConnectorError> {
        let env = create_environment_v3().map_err(|e| e.unwrap()).unwrap();
        let conn = env
            .connect_with_connection_string(&client.get_conn_string())
            .unwrap();

        let temp_table_name = Self::get_stream_temp_table_name(table_name);
        let stream_name = Self::get_stream_table_name(table_name);
        let temp_table_exist = client.table_exist(&conn, &temp_table_name)?;

        if !temp_table_exist {
            let query = format!(
                "CREATE OR REPLACE TEMP TABLE {temp_table_name} AS
                    SELECT * FROM {stream_name} ORDER BY METADATA$ACTION;"
            );

            client.exec(&conn, query)?;
        }

        let result = client.fetch(&conn, format!("SELECT * FROM {temp_table_name};"))?;
        if let Some((schema, iterator)) = result {
            let mut truncated_schema = schema.clone();
            truncated_schema.truncate(schema.len() - 3);

            let columns_length = schema.len();
            let used_columns_for_schema = columns_length - 3;
            let action_idx = used_columns_for_schema;

            for (idx, row) in iterator.enumerate() {
                let ingestion_message = Self::get_ingestion_message(
                    row,
                    action_idx,
                    used_columns_for_schema,
                    table_idx,
                )?;
                ingestor
                    .write()
                    .handle_message(((1, idx as u64), ingestion_message))
                    .map_err(ConnectorError::IngestorError)?;
            }
        }

        let query = format!("DROP TABLE {temp_table_name};");

        client
            .exec(&conn, query)
            .map_err(ConnectorError::SnowflakeError)
            .map(|_| ())
    }
}
