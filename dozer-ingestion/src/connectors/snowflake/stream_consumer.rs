use crate::connectors::snowflake::connection::client::Client;

use crate::connectors::snowflake::snapshotter::Snapshotter;

use crate::errors::ConnectorError;
use crate::ingestion::Ingestor;
use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::log::{debug, error};
use dozer_types::parking_lot::RwLock;

use crate::errors;
use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, OperationEvent, Record, Schema, SchemaIdentifier,
};
use odbc::create_environment_v3;
use std::sync::Arc;

pub struct StreamConsumer {}

impl StreamConsumer {
    pub fn get_stream_table_name(table_name: &String) -> String {
        format!("dozer_{}_stream", table_name)
    }

    pub fn get_stream_temp_table_name(table_name: &String) -> String {
        format!("dozer_{}_stream_temp", table_name)
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
            "CREATE STREAM {} on table {} at(stream => '{}')",
            Self::get_stream_table_name(table_name),
            table_name,
            Snapshotter::get_snapshot_table_name(table_name)
        );
        client
            .exec(&conn, query)
            .map(|_| ())
            .map_err(ConnectorError::SnowflakeError)
    }

    pub fn consume_stream(
        client: &Client,
        connector_id: u64,
        table_name: &String,
        ingestor: &Arc<RwLock<Ingestor>>,
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
                "CREATE OR REPLACE TEMP TABLE {} AS
                    SELECT * FROM {} ORDER BY METADATA$ACTION;",
                temp_table_name, stream_name
            );

            let _r = client.exec(&conn, query);
        }

        let result = client.fetch(&conn, format!("SELECT * FROM {};", temp_table_name));
        match result {
            Ok(Some((schema, iterator))) => {
                let mut truncated_schema = schema.clone();
                truncated_schema.truncate(3);
                ingestor
                    .write()
                    .handle_message((
                        connector_id,
                        IngestionMessage::Schema(
                            table_name.clone(),
                            Schema {
                                identifier: Some(SchemaIdentifier {
                                    id: 10101,
                                    version: 1,
                                }),
                                fields: truncated_schema
                                    .iter()
                                    .map(|c| FieldDefinition {
                                        name: c.name.clone().to_lowercase(),
                                        typ: FieldType::Int,
                                        nullable: None != c.nullable,
                                    })
                                    .collect(),
                                values: vec![],
                                primary_index: vec![0],
                                secondary_indexes: vec![],
                            },
                        ),
                    ))
                    .map_err(errors::ConnectorError::IngestorError)?;

                let columns_length = schema.len();
                let used_columns_for_schema = columns_length - 3;
                let action_idx = used_columns_for_schema;

                iterator.for_each(|row| {
                    if let Some(action) = row.get(action_idx).unwrap() {
                        let mut row_mut = row.clone();

                        let insert_action = "INSERT".to_string();
                        let _delete_action = "DELETE".to_string();

                        let action_a = action.clone();
                        if insert_action == action_a {
                            row_mut.truncate(used_columns_for_schema);
                            ingestor
                                .write()
                                .handle_message((
                                    connector_id,
                                    IngestionMessage::OperationEvent(OperationEvent {
                                        seq_no: 0,
                                        operation: Operation::Insert {
                                            new: Record {
                                                schema_id: Some(SchemaIdentifier {
                                                    id: 10101,
                                                    version: 1,
                                                }),
                                                values: row_mut
                                                    .iter()
                                                    .map(|v| match v {
                                                        None => Field::Null,
                                                        Some(s) => {
                                                            let value: i64 = s.parse().unwrap();
                                                            Field::from(value)
                                                        }
                                                    })
                                                    .collect(),
                                            },
                                        },
                                    }),
                                ))
                                .unwrap();
                        // } else if update_action == action_a {
                        //     c.truncate(used_columns_for_schema);
                        //     ingestor
                        //         .write()
                        //         .handle_message((
                        //             connector_id,
                        //             IngestionMessage::OperationEvent(OperationEvent {
                        //                 seq_no: 0,
                        //                 operation: Operation::Delete {
                        //                     old: Record {
                        //                         schema_id: Some(SchemaIdentifier {
                        //                             id: 10101,
                        //                             version: 1,
                        //                         }),
                        //                         values: c
                        //                             .iter()
                        //                             .map(|v| match v {
                        //                                 None => Field::Null,
                        //                                 Some(s) => {
                        //                                     let value: i64 = s.parse().unwrap();
                        //                                     Field::from(value)
                        //                                 }
                        //                             })
                        //                             .collect(),
                        //                     },
                        //                 },
                        //             }),
                        //         ))
                        //         .unwrap();
                        } else {
                            error!("Not supposed to be here");
                        }
                    }
                })
            }
            Err(_) => {
                debug!("error");
            }
            _ => {
                debug!("other");
            }
        };

        debug!("consumed");

        Ok(())
    }
}
