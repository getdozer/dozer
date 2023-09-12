use crate::connectors::{ListOrFilterColumns, SourceSchemaResult};
use crate::ingestion::Ingestor;

use super::helper;
use crate::connectors::postgres::connection::helper as connection_helper;
use crate::errors::ConnectorError;
use crate::errors::PostgresConnectorError::SyncWithSnapshotError;
use crate::errors::PostgresConnectorError::{InvalidQueryError, PostgresSchemaError};

use crate::connectors::postgres::schema::helper::SchemaHelper;
use crate::errors::ConnectorError::PostgresConnectorError;
use dozer_types::types::Schema;

use dozer_types::ingestion_types::IngestionMessage;

use dozer_types::types::Operation;
use futures::StreamExt;
use tokio::sync::mpsc::{channel, Sender};
use tokio::task::JoinSet;

pub struct PostgresSnapshotter<'a> {
    pub conn_config: tokio_postgres::Config,
    pub ingestor: &'a Ingestor,
}

impl<'a> PostgresSnapshotter<'a> {
    pub async fn get_tables(
        &self,
        tables: &[ListOrFilterColumns],
    ) -> Result<Vec<SourceSchemaResult>, ConnectorError> {
        let helper = SchemaHelper::new(self.conn_config.clone());
        helper
            .get_schemas(tables)
            .await
            .map_err(PostgresConnectorError)
    }

    pub async fn sync_table(
        schema: Schema,
        schema_name: String,
        table_name: String,
        table_index: usize,
        conn_config: tokio_postgres::Config,
        sender: Sender<Result<(usize, Operation), ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        let mut client_plain = connection_helper::connect(conn_config)
            .await
            .map_err(PostgresConnectorError)?;

        let column_str: Vec<String> = schema
            .fields
            .iter()
            .map(|f| format!("\"{0}\"", f.name))
            .collect();

        let column_str = column_str.join(",");
        let query = format!("select {column_str} from {schema_name}.{table_name}");
        let stmt = client_plain
            .prepare(&query)
            .await
            .map_err(|e| PostgresConnectorError(InvalidQueryError(e)))?;
        let columns = stmt.columns();

        let empty_vec: Vec<String> = Vec::new();
        let row_stream = client_plain
            .query_raw(query, empty_vec)
            .await
            .map_err(|e| PostgresConnectorError(InvalidQueryError(e)))?;
        tokio::pin!(row_stream);
        while let Some(msg) = row_stream.next().await {
            match msg {
                Ok(msg) => {
                    let evt = helper::map_row_to_operation_event(&msg, columns)
                        .map_err(|e| PostgresConnectorError(PostgresSchemaError(e)))?;

                    let Ok(_) = sender.send(Ok((table_index, evt))).await else {
                        // If we can't send, the parent task has quit. There is
                        // no use in going on, but if there was an error, it was
                        // handled by the parent.
                        return Ok(());
                    };
                }
                Err(e) => return Err(PostgresConnectorError(SyncWithSnapshotError(e.to_string()))),
            }
        }

        Ok(())
    }

    pub async fn sync_tables(&self, tables: &[ListOrFilterColumns]) -> Result<(), ConnectorError> {
        let schemas = self.get_tables(tables).await?;

        let (tx, mut rx) = channel(16);

        let mut joinset = JoinSet::new();
        for (table_index, (schema, table)) in schemas.into_iter().zip(tables).enumerate() {
            let schema = schema?;
            let schema = schema.schema;
            let schema_name = table.schema.clone().unwrap_or("public".to_string());
            let table_name = table.name.clone();
            let conn_config = self.conn_config.clone();
            let sender = tx.clone();
            joinset.spawn(async move {
                if let Err(e) = Self::sync_table(
                    schema,
                    schema_name,
                    table_name,
                    table_index,
                    conn_config,
                    sender.clone(),
                )
                .await
                {
                    sender.send(Err(e)).await.unwrap();
                }
            });
        }
        // Make sure the last sender is dropped so receiving on the channel doesn't
        // deadlock
        drop(tx);

        self.ingestor
            .handle_message(IngestionMessage::SnapshottingStarted)
            .await
            .map_err(|_| ConnectorError::IngestorError)?;

        while let Some(message) = rx.recv().await {
            let (table_index, evt) = message?;
            self.ingestor
                .handle_message(IngestionMessage::OperationEvent {
                    table_index,
                    op: evt,
                    id: None,
                })
                .await
                .map_err(|_| ConnectorError::IngestorError)?;
        }

        self.ingestor
            .handle_message(IngestionMessage::SnapshottingDone)
            .await
            .map_err(|_| ConnectorError::IngestorError)?;

        // All tasks in the joinset should have finished (because they have dropped their senders)
        // Otherwise, they will be aborted when the joinset is dropped
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rand::Rng;
    use serial_test::serial;

    use crate::{
        connectors::{
            postgres::{
                connection::helper::map_connection_config, tests::client::TestPostgresClient,
            },
            ListOrFilterColumns,
        },
        errors::ConnectorError,
        ingestion::{IngestionConfig, Ingestor},
        test_util::run_connector_test,
    };

    use super::PostgresSnapshotter;

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_snapshotter_sync_tables_successfully_1_requested_table() {
        run_connector_test("postgres", |app_config| async move {
            let config = app_config
                .connections
                .get(0)
                .unwrap()
                .config
                .as_ref()
                .unwrap();

            let mut test_client = TestPostgresClient::new(config).await;

            let mut rng = rand::thread_rng();
            let table_name = format!("test_table_{}", rng.gen::<u32>());

            test_client.create_simple_table("public", &table_name).await;
            test_client.insert_rows(&table_name, 2, None).await;

            let conn_config = map_connection_config(config).unwrap();

            let input_tables = vec![ListOrFilterColumns {
                name: table_name,
                schema: Some("public".to_string()),
                columns: None,
            }];

            let ingestion_config = IngestionConfig::default();
            let (ingestor, mut iterator) = Ingestor::initialize_channel(ingestion_config);

            let snapshotter = PostgresSnapshotter {
                conn_config,
                ingestor: &ingestor,
            };

            let actual = snapshotter.sync_tables(&input_tables).await;

            assert!(actual.is_ok());

            let mut i = 0;
            while i < 2 {
                if iterator
                    .next_timeout(Duration::from_secs(1))
                    .await
                    .is_none()
                {
                    panic!("Unexpected operation");
                }
                i += 1;
            }
        })
        .await
    }

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_snapshotter_sync_tables_successfully_not_match_table() {
        run_connector_test("postgres", |app_config| async move {
            let config = app_config
                .connections
                .get(0)
                .unwrap()
                .config
                .as_ref()
                .unwrap();

            let mut test_client = TestPostgresClient::new(config).await;

            let mut rng = rand::thread_rng();
            let table_name = format!("test_table_{}", rng.gen::<u32>());

            test_client.create_simple_table("public", &table_name).await;
            test_client.insert_rows(&table_name, 2, None).await;

            let conn_config = map_connection_config(config).unwrap();

            let input_table_name = String::from("not_existing_table");
            let input_tables = vec![ListOrFilterColumns {
                name: input_table_name,
                schema: Some("public".to_string()),
                columns: None,
            }];

            let ingestion_config = IngestionConfig::default();
            let (ingestor, mut _iterator) = Ingestor::initialize_channel(ingestion_config);

            let snapshotter = PostgresSnapshotter {
                conn_config,
                ingestor: &ingestor,
            };

            let actual = snapshotter.sync_tables(&input_tables).await;

            assert!(actual.is_err());

            match actual {
                Ok(_) => panic!("Test failed"),
                Err(e) => {
                    assert!(matches!(e, ConnectorError::PostgresConnectorError(_)));
                }
            }
        })
        .await
    }

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_snapshotter_sync_tables_successfully_table_not_exist() {
        run_connector_test("postgres", |app_config| async move {
            let config = app_config
                .connections
                .get(0)
                .unwrap()
                .config
                .as_ref()
                .unwrap();

            let mut rng = rand::thread_rng();
            let table_name = format!("test_table_{}", rng.gen::<u32>());

            let conn_config = map_connection_config(config).unwrap();

            let input_tables = vec![ListOrFilterColumns {
                name: table_name,
                schema: Some("public".to_string()),
                columns: None,
            }];

            let ingestion_config = IngestionConfig::default();
            let (ingestor, mut _iterator) = Ingestor::initialize_channel(ingestion_config);

            let snapshotter = PostgresSnapshotter {
                conn_config,
                ingestor: &ingestor,
            };

            let actual = snapshotter.sync_tables(&input_tables).await;

            assert!(actual.is_err());

            match actual {
                Ok(_) => panic!("Test failed"),
                Err(e) => {
                    assert!(matches!(e, ConnectorError::PostgresConnectorError(_)));
                }
            }
        })
        .await
    }
}
