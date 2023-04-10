#[cfg(test)]
mod tests {
    use crate::connectors::postgres::connection::helper;
    use crate::connectors::postgres::connection::helper::map_connection_config;
    use crate::connectors::postgres::connector::{PostgresConfig, PostgresConnector};
    use crate::connectors::postgres::replication_slot_helper::ReplicationSlotHelper;
    use crate::connectors::postgres::test_utils::{create_slot, retry_drop_active_slot};
    use crate::connectors::postgres::tests::client::TestPostgresClient;
    use crate::connectors::TableIdentifier;
    // use crate::connectors::Connector;
    // use crate::ingestion::IngestionConfig;
    use crate::test_util::run_connector_test;
    // use dozer_types::ingestion_types::IngestionMessage;
    // use dozer_types::node::OpIdentifier;
    use rand::Rng;
    use serial_test::serial;
    use tokio_postgres::config::ReplicationMode;

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_continue_replication() {
        run_connector_test("postgres", |app_config| async move {
            let config = app_config
                .connections
                .get(0)
                .unwrap()
                .config
                .as_ref()
                .unwrap();
            let conn_config = map_connection_config(config).unwrap();
            let postgres_config = PostgresConfig {
                name: "test".to_string(),
                config: conn_config.clone(),
            };

            let connector = PostgresConnector::new(postgres_config);

            // let result = connector.can_start_from((1, 0)).unwrap();
            // assert!(!result, "Cannot continue, because slot doesnt exist");

            let mut replication_conn_config = conn_config;
            replication_conn_config.replication_mode(ReplicationMode::Logical);

            // Creating publication
            let client = helper::connect(replication_conn_config.clone())
                .await
                .unwrap();
            connector.create_publication(client, None).await.unwrap();

            // Creating slot
            let client = helper::connect(replication_conn_config.clone())
                .await
                .unwrap();
            let slot_name = connector.get_slot_name();
            let _parsed_lsn = create_slot(&client, &slot_name).await;

            // let result = connector
            //     .can_start_from((u64::from(parsed_lsn), 0))
            //     .unwrap();

            ReplicationSlotHelper::drop_replication_slot(&client, &slot_name)
                .await
                .unwrap();
            // assert!(
            //     result,
            //     "Replication slot is created and it should be possible to continue"
            // );
        })
        .await
    }

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_continue_replication_from_lsn() {
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
            let connector_name = format!("pg_connector_{}", rng.gen::<u32>());
            test_client.create_simple_table("public", &table_name).await;

            let conn_config = map_connection_config(config).unwrap();
            let postgres_config = PostgresConfig {
                name: connector_name,
                config: conn_config.clone(),
            };

            let connector = PostgresConnector::new(postgres_config);

            let mut replication_conn_config = conn_config;
            replication_conn_config.replication_mode(ReplicationMode::Logical);

            // Creating publication
            let client = helper::connect(replication_conn_config.clone())
                .await
                .unwrap();
            let table_identifier = TableIdentifier {
                schema: Some("public".to_string()),
                name: table_name.clone(),
            };
            connector
                .create_publication(client, Some(&[table_identifier]))
                .await
                .unwrap();

            // Creating slot
            let client = helper::connect(replication_conn_config.clone())
                .await
                .unwrap();

            let slot_name = connector.get_slot_name();
            let _parsed_lsn = create_slot(&client, &slot_name).await;

            // let config = IngestionConfig::default();
            // let (ingestor, mut iterator) = Ingestor::initialize_channel(config);

            test_client.insert_rows(&table_name, 4, None).await;

            // assume that we already received two rows
            // let last_parsed_position = 2_u64;
            // thread::spawn(move || {
            //     let connector = PostgresConnector::new(postgres_config);
            //     let _ = connector.start(
            //         Some((u64::from(parsed_lsn), last_parsed_position)),
            //         &ingestor,
            //         tables,
            //     );
            // });

            // let mut i = last_parsed_position;
            // while i < 4 {
            //     i += 1;
            //     if let Some(IngestionMessage {
            //         identifier: OpIdentifier { seq_in_tx, .. },
            //         ..
            //     }) = iterator.next()
            //     {
            //         assert_eq!(i, seq_in_tx);
            //     } else {
            //         panic!("Unexpected operation");
            //     }
            // }

            // test_client.insert_rows(&table_name, 3, None);
            // let mut i = 0;
            // while i < 3 {
            //     i += 1;
            //     if let Some(IngestionMessage {
            //         identifier: OpIdentifier { seq_in_tx, .. },
            //         ..
            //     }) = iterator.next()
            //     {
            //         assert_eq!(i, seq_in_tx);
            //     } else {
            //         panic!("Unexpected operation");
            //     }
            // }

            if let Err(e) = ReplicationSlotHelper::drop_replication_slot(&client, &slot_name).await
            {
                retry_drop_active_slot(e, &client, &slot_name)
                    .await
                    .unwrap();
            }
        })
        .await
    }
}
