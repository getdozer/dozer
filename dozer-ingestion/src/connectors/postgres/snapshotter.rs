use crate::connectors::{ListOrFilterColumns, SourceSchemaResult};
use crate::ingestion::Ingestor;

use super::helper;
use crate::connectors::postgres::connection::helper as connection_helper;
use crate::errors::ConnectorError;
use crate::errors::PostgresConnectorError::{InvalidQueryError, PostgresSchemaError};
use crate::errors::PostgresConnectorError::{SnapshotReadError, SyncWithSnapshotError};

use crate::connectors::postgres::schema::helper::SchemaHelper;
use crate::errors::ConnectorError::PostgresConnectorError;
use dozer_types::types::Schema;

use dozer_types::ingestion_types::IngestionMessage;

use dozer_types::types::Operation;
use futures::StreamExt;
use tokio::sync::mpsc::{channel, Sender};

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
        name: String,
        conn_config: tokio_postgres::Config,
        sender: Sender<Result<Option<Operation>, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        let client_plain = connection_helper::connect(conn_config)
            .await
            .map_err(PostgresConnectorError)?;

        let column_str: Vec<String> = schema
            .fields
            .iter()
            .map(|f| format!("\"{0}\"", f.name))
            .collect();

        let column_str = column_str.join(",");
        let query = format!("select {column_str} from {schema_name}.{name}");
        let stmt = client_plain
            .prepare(&query)
            .await
            .map_err(|e| PostgresConnectorError(InvalidQueryError(e)))?;
        let columns = stmt.columns();

        let empty_vec: Vec<String> = Vec::new();
        let row_stream = client_plain
            .query_raw(&stmt, empty_vec)
            .await
            .map_err(|e| PostgresConnectorError(InvalidQueryError(e)))?;
        tokio::pin!(row_stream);
        while let Some(msg) = row_stream.next().await {
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

                    sender.send(Ok(Some(evt))).await.unwrap();
                }
                Err(e) => return Err(PostgresConnectorError(SyncWithSnapshotError(e.to_string()))),
            }
        }

        // After table read is finished, send None as message to inform receiver loop about end of table
        sender.send(Ok(None)).await.unwrap();
        Ok(())
    }

    pub async fn sync_tables(&self, tables: &[ListOrFilterColumns]) -> Result<(), ConnectorError> {
        let schemas = self.get_tables(tables).await?;

        let mut left_tables_count = tables.len();

        let (tx, mut rx) = channel(16);

        for (schema, table) in schemas.into_iter().zip(tables) {
            let schema = schema?;
            let schema = schema.schema;
            let schema_name = table.schema.clone().unwrap_or("public".to_string());
            let name = table.name.clone();
            let conn_config = self.conn_config.clone();
            let sender = tx.clone();
            tokio::spawn(async move {
                if let Err(e) =
                    Self::sync_table(schema, schema_name, name, conn_config, sender.clone()).await
                {
                    sender.send(Err(e)).await.unwrap();
                }
            });
        }

        self.ingestor
            .handle_message(IngestionMessage::new_snapshotting_started(0_u64, 0))
            .map_err(ConnectorError::IngestorError)?;
        let mut idx = 1;
        loop {
            let message = rx
                .recv()
                .await
                .ok_or(PostgresConnectorError(SnapshotReadError))??;
            match message {
                None => {
                    left_tables_count -= 1;
                    if left_tables_count == 0 {
                        break;
                    }
                }
                Some(evt) => {
                    self.ingestor
                        .handle_message(IngestionMessage::new_op(0, idx, evt))
                        .map_err(ConnectorError::IngestorError)?;
                    idx += 1;
                }
            }
        }

        self.ingestor
            .handle_message(IngestionMessage::new_snapshotting_done(0_u64, idx))
            .map_err(ConnectorError::IngestorError)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use dozer_types::{ingestion_types::IngestionMessage, node::OpIdentifier};
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

            let test_client = TestPostgresClient::new(config).await;

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
                if let Some(IngestionMessage {
                    identifier: OpIdentifier { seq_in_tx, .. },
                    ..
                }) = iterator.next()
                {
                    assert_eq!(i, seq_in_tx);
                } else {
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

            let test_client = TestPostgresClient::new(config).await;

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
