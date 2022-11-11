use crate::connectors::snowflake::connection::client::Client;
use crate::errors;
use crate::errors::ConnectorError;
use crate::ingestion::Ingestor;
use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::log::debug;
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{
    Field, Operation, OperationEvent, Record, SchemaIdentifier,
};

use crate::connectors::snowflake::schema_helper::SchemaHelper;
use odbc::create_environment_v3;
use std::sync::Arc;

pub struct Snapshotter {}

impl Snapshotter {
    pub fn get_snapshot_table_name(table_name: &String) -> String {
        format!("dozer_{}_snapshot", table_name)
    }

    pub fn run(
        client: &Client,
        ingestor: &Arc<RwLock<Ingestor>>,
        connector_id: u64,
        table_name: String,
    ) -> Result<(), ConnectorError> {
        let env = create_environment_v3().map_err(|e| e.unwrap()).unwrap();
        let conn = env
            .connect_with_connection_string(&client.get_conn_string())
            .unwrap();

        let query = format!(
            "CREATE STREAM IF NOT EXISTS {} ON TABLE {} SHOW_INITIAL_ROWS = TRUE;",
            Snapshotter::get_snapshot_table_name(&table_name),
            table_name
        );
        client.exec(&conn, query)?;

        let result = client.fetch(&conn, format!("SELECT * FROM {};", table_name));
        match result {
            Ok(Some((schema, iterator))) => {
                ingestor
                    .write()
                    .handle_message((
                        connector_id,
                        IngestionMessage::Schema(table_name, SchemaHelper::map_schema(schema)?),
                    ))
                    .map_err(errors::ConnectorError::IngestorError)?;
                iterator.for_each(|values| {
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
                                        values: values
                                            .iter()
                                            .map(|v| match v {
                                                None => Field::Null,
                                                Some(s) => s.clone(),
                                            })
                                            .collect(),
                                    },
                                },
                            }),
                        ))
                        .unwrap();
                });
            }
            Err(_) => {
                debug!("error");
            }
            _ => {
                debug!("other");
            }
        }

        Ok(())
    }
}
