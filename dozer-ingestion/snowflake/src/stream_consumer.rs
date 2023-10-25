use dozer_ingestion_connector::{
    dozer_types::{
        models::ingestion_types::IngestionMessage,
        node::OpIdentifier,
        types::{Field, Operation, Record},
    },
    Ingestor,
};

use crate::{connection::client::Client, SnowflakeError, SnowflakeStreamError};

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

    pub fn is_stream_created(client: &Client, table_name: &str) -> Result<bool, SnowflakeError> {
        client.stream_exist(&Self::get_stream_table_name(table_name, &client.get_name()))
    }

    pub fn drop_stream(client: &Client, table_name: &str) -> Result<(), SnowflakeError> {
        let query = format!(
            "DROP STREAM IF EXISTS {}",
            Self::get_stream_table_name(table_name, &client.get_name()),
        );

        client.exec(&query)
    }

    pub fn create_stream(client: &Client, table_name: &String) -> Result<(), SnowflakeError> {
        let query = format!(
            "CREATE STREAM {} on table {} SHOW_INITIAL_ROWS = TRUE",
            Self::get_stream_table_name(table_name, &client.get_name()),
            table_name,
        );

        let result = client.exec_stream_creation(query)?;

        if !result {
            let query = format!(
                "CREATE STREAM {} on view {} SHOW_INITIAL_ROWS = TRUE",
                Self::get_stream_table_name(table_name, &client.get_name()),
                table_name
            );
            client.exec(&query)?;
        }

        Ok(())
    }

    fn get_operation(
        row: Vec<Field>,
        action_idx: usize,
        used_columns_for_schema: usize,
    ) -> Result<Operation, SnowflakeError> {
        if let Field::String(action) = row.get(action_idx).unwrap() {
            let mut row_mut = row.clone();
            let insert_action = &"INSERT";
            let delete_action = &"DELETE";

            row_mut.truncate(used_columns_for_schema);

            if insert_action == action {
                Ok(Operation::Insert {
                    new: Record::new(row_mut),
                })
            } else if delete_action == action {
                Ok(Operation::Delete {
                    old: Record::new(row_mut),
                })
            } else {
                Err(SnowflakeError::SnowflakeStreamError(
                    SnowflakeStreamError::UnsupportedActionInStream(action.clone()),
                ))
            }
        } else {
            Err(SnowflakeError::SnowflakeStreamError(
                SnowflakeStreamError::CannotDetermineAction,
            ))
        }
    }

    pub fn consume_stream(
        &mut self,
        client: &Client,
        table_name: &str,
        ingestor: &Ingestor,
        table_index: usize,
        iteration: u64,
    ) -> Result<(), SnowflakeError> {
        let temp_table_name = Self::get_stream_temp_table_name(table_name, &client.get_name());
        let stream_name = Self::get_stream_table_name(table_name, &client.get_name());

        let query = format!(
            "CREATE TEMP TABLE IF NOT EXISTS {temp_table_name} AS
                    SELECT * FROM {stream_name} ORDER BY METADATA$ACTION;"
        );

        client.exec(&query)?;

        let rows = client.fetch(format!("SELECT * FROM {temp_table_name};"))?;
        if let Some(schema) = rows.schema() {
            let schema_len = schema.len();
            let mut truncated_schema = schema.clone();
            truncated_schema.truncate(schema_len - 3);

            let columns_length = schema_len;
            let used_columns_for_schema = columns_length - 3;
            let action_idx = used_columns_for_schema;

            for (idx, result) in rows.enumerate() {
                let row = result?;
                let op = Self::get_operation(row, action_idx, used_columns_for_schema)?;
                if ingestor
                    .blocking_handle_message(IngestionMessage::OperationEvent {
                        table_index,
                        op,
                        id: Some(OpIdentifier::new(iteration, idx as u64)),
                    })
                    .is_err()
                {
                    // If receiver is dropped, we can stop processing
                    return Ok(());
                }
            }
        }

        let query = format!("DROP TABLE {temp_table_name};");

        client.exec(&query)
    }
}
