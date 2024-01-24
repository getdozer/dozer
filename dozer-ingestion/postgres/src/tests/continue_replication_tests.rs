#[cfg(test)]
mod tests {
    use dozer_ingestion_connector::{tokio, TableIdentifier};
    // use crate::connectors::Connector;
    // use crate::ingestion::IngestionConfig;
    // use dozer_types::models::ingestion_types::IngestionMessage;
    // use dozer_types::node::OpIdentifier;
    use rand::Rng;
    use serial_test::serial;
    use tokio_postgres::config::ReplicationMode;

    use crate::{
        connection::helper::{self, map_connection_config},
        connector::{create_publication, PostgresConfig, PostgresConnector},
        replication_slot_helper::ReplicationSlotHelper,
        test_utils::{create_slot, load_test_connection_config, retry_drop_active_slot},
        tests::client::TestPostgresClient,
    };

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_continue_replication() {
        let config = load_test_connection_config().await;
        let conn_config = map_connection_config(&config).unwrap();
        let postgres_config = PostgresConfig {
            name: "test".to_string(),
            config: conn_config.clone(),
            schema: None,
            batch_size: 1000,
        };

        let connector = PostgresConnector::new(postgres_config, None).unwrap();

        // let result = connector.can_start_from((1, 0)).unwrap();
        // assert!(!result, "Cannot continue, because slot doesnt exist");

        let mut replication_conn_config = conn_config;
        replication_conn_config.replication_mode(ReplicationMode::Logical);

        // Creating publication
        let client = helper::connect(replication_conn_config.clone())
            .await
            .unwrap();
        create_publication(client, &connector.name, None)
            .await
            .unwrap();

        // Creating slot
        let mut client = helper::connect(replication_conn_config.clone())
            .await
            .unwrap();
        let _parsed_lsn = create_slot(&mut client, &connector.slot_name).await;

        // let result = connector
        //     .can_start_from((u64::from(parsed_lsn), 0))
        //     .unwrap();

        ReplicationSlotHelper::drop_replication_slot(&mut client, &connector.slot_name)
            .await
            .unwrap();
        // assert!(
        //     result,
        //     "Replication slot is created and it should be possible to continue"
        // );
    }

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_continue_replication_from_lsn() {
        let config = load_test_connection_config().await;

        let mut test_client = TestPostgresClient::new(&config).await;
        let mut rng = rand::thread_rng();
        let table_name = format!("test_table_{}", rng.gen::<u32>());
        let connector_name = format!("pg_connector_{}", rng.gen::<u32>());
        test_client.create_simple_table("public", &table_name).await;

        let conn_config = map_connection_config(&config).unwrap();
        let postgres_config = PostgresConfig {
            name: connector_name,
            config: conn_config.clone(),
            schema: None,
            batch_size: 1000,
        };

        let connector = PostgresConnector::new(postgres_config, None).unwrap();

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
        create_publication(client, &connector.name, Some(&[table_identifier]))
            .await
            .unwrap();

        // Creating slot
        let mut client = helper::connect(replication_conn_config.clone())
            .await
            .unwrap();

        let _parsed_lsn = create_slot(&mut client, &connector.slot_name).await;

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

        if let Err(e) =
            ReplicationSlotHelper::drop_replication_slot(&mut client, &connector.slot_name).await
        {
            retry_drop_active_slot(e, &mut client, &connector.slot_name)
                .await
                .unwrap();
        }
    }
}
