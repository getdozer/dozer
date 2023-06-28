use crate::connectors::snowflake::connection::client::Client;

use crate::errors::{ConnectorError, SnowflakeError};
use crate::ingestion::Ingestor;
use dozer_types::ingestion_types::IngestionMessage;

use crate::errors::SnowflakeStreamError::{CannotDetermineAction, UnsupportedActionInStream};
use dozer_types::types::{Field, Operation, Record, SchemaIdentifier};
use odbc::create_environment_v3;

#[derive(Default)]
pub struct StreamConsumer {}

impl StreamConsumer {
    pub fn new() -> Self {
        Self {}
    }

    pub fn get_stream_table_name(table_name: &str, client_name: &str) -> String {
        format!("dozer_{table_name}_{client_name}_stream")
    }

    pub fn get_stream_temp_table_name(table_name: &str, client_name: &str) -> String {
        format!("dozer_{table_name}_{client_name}_stream_temp")
    }

    pub fn is_stream_created(client: &Client, table_name: &str) -> Result<bool, ConnectorError> {
        let env = create_environment_v3().map_err(|e| e.unwrap()).unwrap();
        let conn = env
            .connect_with_connection_string(&client.get_conn_string())
            .unwrap();

        client
            .stream_exist(&conn, &Self::get_stream_table_name(table_name, &client.get_name()))
            .map_err(ConnectorError::SnowflakeError)
    }

    pub fn drop_stream(client: &Client, table_name: &str) -> Result<Option<bool>, SnowflakeError> {
        let env = create_environment_v3().map_err(|e| e.unwrap()).unwrap();
        let conn = env
            .connect_with_connection_string(&client.get_conn_string())
            .unwrap();

        let query = format!(
            "DROP STREAM IF EXISTS {}",
            Self::get_stream_table_name(table_name, &client.get_name()),
        );

        client.exec(&conn, query)
    }

    pub fn create_stream(client: &Client, table_name: &String) -> Result<(), ConnectorError> {
        let env = create_environment_v3().map_err(|e| e.unwrap()).unwrap();
        let conn = env
            .connect_with_connection_string(&client.get_conn_string())
            .unwrap();

        let query = format!(
            "CREATE STREAM {} on table {} SHOW_INITIAL_ROWS = TRUE",
            Self::get_stream_table_name(table_name, &client.get_name()),
            table_name,
        );

        let result = client.exec_stream_creation(&conn, query)?;

        if !result {
            let query = format!(
                "CREATE STREAM {} on view {} SHOW_INITIAL_ROWS = TRUE",
                Self::get_stream_table_name(table_name, &client.get_name()),
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
            lifetime: None,
        }
    }

    fn get_operation(
        row: Vec<Field>,
        action_idx: usize,
        used_columns_for_schema: usize,
        table_idx: usize,
    ) -> Result<Operation, ConnectorError> {
        if let Field::String(action) = row.get(action_idx).unwrap() {
            let mut row_mut = row.clone();
            let insert_action = &"INSERT";
            let delete_action = &"DELETE";

            row_mut.truncate(used_columns_for_schema);

            if insert_action == action {
                Ok(Operation::Insert {
                    new: Self::map_record(row_mut, table_idx),
                })
            } else if delete_action == action {
                Ok(Operation::Delete {
                    old: Self::map_record(row_mut, table_idx),
                })
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
        ingestor: &Ingestor,
        table_idx: usize,
        iteration: u64,
    ) -> Result<(), ConnectorError> {
        let env = create_environment_v3().map_err(|e| e.unwrap()).unwrap();
        let conn = env
            .connect_with_connection_string(&client.get_conn_string())
            .unwrap();

        let temp_table_name = Self::get_stream_temp_table_name(table_name, &client.get_name());
        let stream_name = Self::get_stream_table_name(table_name, &client.get_name());
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
                let op = Self::get_operation(row, action_idx, used_columns_for_schema, table_idx)?;
                ingestor
                    .handle_message(IngestionMessage::new_op(iteration, idx as u64, op))
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
