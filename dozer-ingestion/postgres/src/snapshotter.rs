use dozer_ingestion_connector::{
    dozer_types::{
        models::ingestion_types::IngestionMessage,
        types::{Operation, Schema},
    },
    futures::StreamExt,
    tokio::{
        self,
        sync::mpsc::{channel, Sender},
        task::JoinSet,
    },
    utils::ListOrFilterColumns,
    Ingestor, SourceSchema,
};

use crate::{
    connection::helper as connection_helper, schema::helper::SchemaHelper, PostgresConnectorError,
};

use super::helper;

pub struct PostgresSnapshotter<'a> {
    pub conn_config: tokio_postgres::Config,
    pub ingestor: &'a Ingestor,
    pub schema: Option<String>,
}

impl<'a> PostgresSnapshotter<'a> {
    pub async fn get_tables(
        &self,
        tables: &[ListOrFilterColumns],
    ) -> Result<Vec<Result<SourceSchema, PostgresConnectorError>>, PostgresConnectorError> {
        let helper = SchemaHelper::new(self.conn_config.clone(), self.schema.clone());
        helper.get_schemas(tables).await
    }

    pub async fn sync_table(
        schema: Schema,
        schema_name: String,
        table_name: String,
        table_index: usize,
        conn_config: tokio_postgres::Config,
        sender: Sender<Result<(usize, Operation), PostgresConnectorError>>,
    ) -> Result<(), PostgresConnectorError> {
        let mut client_plain = connection_helper::connect(conn_config).await?;

        let column_str: Vec<String> = schema
            .fields
            .iter()
            .map(|f| format!("\"{0}\"", f.name))
            .collect();

        let column_str = column_str.join(",");
        let query = format!(r#"select {column_str} from "{schema_name}"."{table_name}""#);
        let stmt = client_plain
            .prepare(&query)
            .await
            .map_err(PostgresConnectorError::InvalidQueryError)?;
        let columns = stmt.columns();

        let empty_vec: Vec<String> = Vec::new();
        let row_stream = client_plain
            .query_raw(query, empty_vec)
            .await
            .map_err(PostgresConnectorError::InvalidQueryError)?;
        tokio::pin!(row_stream);
        while let Some(msg) = row_stream.next().await {
            match msg {
                Ok(msg) => {
                    let evt = helper::map_row_to_operation_event(&msg, columns)
                        .map_err(PostgresConnectorError::PostgresSchemaError)?;

                    let Ok(_) = sender.send(Ok((table_index, evt))).await else {
                        // If we can't send, the parent task has quit. There is
                        // no use in going on, but if there was an error, it was
                        // handled by the parent.
                        return Ok(());
                    };
                }
                Err(e) => return Err(PostgresConnectorError::SyncWithSnapshotError(e.to_string())),
            }
        }

        Ok(())
    }

    pub async fn sync_tables(
        &self,
        tables: &[ListOrFilterColumns],
    ) -> Result<(), PostgresConnectorError> {
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

        if self
            .ingestor
            .handle_message(IngestionMessage::SnapshottingStarted)
            .await
            .is_err()
        {
            // If receiving side is closed, we can stop
            return Ok(());
        }

        while let Some(message) = rx.recv().await {
            let (table_index, evt) = message?;
            if self
                .ingestor
                .handle_message(IngestionMessage::OperationEvent {
                    table_index,
                    op: evt,
                    id: None,
                })
                .await
                .is_err()
            {
                // If receiving side is closed, we can stop
                return Ok(());
            }
        }

        if self
            .ingestor
            .handle_message(IngestionMessage::SnapshottingDone)
            .await
            .is_err()
        {
            // If receiving side is closed, we can stop
            return Ok(());
        }

        // All tasks in the joinset should have finished (because they have dropped their senders)
        // Otherwise, they will be aborted when the joinset is dropped
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use dozer_ingestion_connector::{tokio, utils::ListOrFilterColumns, IngestionConfig, Ingestor};
    use rand::Rng;
    use serial_test::serial;

    use crate::{
        connection::helper::map_connection_config, test_utils::load_test_connection_config,
        tests::client::TestPostgresClient,
    };

    use super::PostgresSnapshotter;

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_snapshotter_sync_tables_successfully_1_requested_table() {
        let config = load_test_connection_config().await;

        let mut test_client = TestPostgresClient::new(&config).await;

        let mut rng = rand::thread_rng();
        let table_name = format!("test_table_{}", rng.gen::<u32>());

        test_client.create_simple_table("public", &table_name).await;
        test_client.insert_rows(&table_name, 2, None).await;

        let conn_config = map_connection_config(&config).unwrap();

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
            schema: None,
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
    }

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_snapshotter_sync_tables_successfully_not_match_table() {
        let config = load_test_connection_config().await;

        let mut test_client = TestPostgresClient::new(&config).await;

        let mut rng = rand::thread_rng();
        let table_name = format!("test_table_{}", rng.gen::<u32>());

        test_client.create_simple_table("public", &table_name).await;
        test_client.insert_rows(&table_name, 2, None).await;

        let conn_config = map_connection_config(&config).unwrap();

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
            schema: None,
        };

        let actual = snapshotter.sync_tables(&input_tables).await;

        assert!(actual.is_err());
    }

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_snapshotter_sync_tables_successfully_table_not_exist() {
        let config = load_test_connection_config().await;

        let mut rng = rand::thread_rng();
        let table_name = format!("test_table_{}", rng.gen::<u32>());

        let conn_config = map_connection_config(&config).unwrap();

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
            schema: None,
        };

        let actual = snapshotter.sync_tables(&input_tables).await;

        assert!(actual.is_err());
    }
}
