#[cfg(test)]
mod tests {
    use crate::connectors::postgres::connection::helper;
    use crate::connectors::postgres::connection::helper::map_connection_config;
    use crate::connectors::postgres::connector::{PostgresConfig, PostgresConnector};
    use crate::connectors::postgres::test_utils::create_slot;
    use crate::connectors::postgres::tests::client::TestPostgresClient;
    use crate::connectors::Connector;
    use crate::connectors::TableInfo;
    use crate::ingestion::{IngestionConfig, Ingestor};
    use crate::test_util::run_connector_test;
    use core::cell::RefCell;
    use rand::Rng;
    use serial_test::serial;
    use std::sync::Arc;
    use std::thread;
    use tokio_postgres::config::ReplicationMode;

    #[test]
    #[serial]
    #[ignore]
    fn test_connector_continue_replication() {
        run_connector_test("postgres", |app_config| {
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
                tables: None,
                config: conn_config.clone(),
            };

            let connector = PostgresConnector::new(1, postgres_config);

            let result = connector.can_start_from((1, 0)).unwrap();
            assert!(!result, "Cannot continue, because slot doesnt exist");

            let mut replication_conn_config = conn_config;
            replication_conn_config.replication_mode(ReplicationMode::Logical);

            // Creating publication
            let client = helper::connect(replication_conn_config.clone()).unwrap();
            connector.create_publication(client).unwrap();

            // Creating slot
            let client = helper::connect(replication_conn_config.clone()).unwrap();
            let client_ref = Arc::new(RefCell::new(client));
            let slot_name = connector.get_slot_name();
            let parsed_lsn = create_slot(client_ref, &slot_name);

            let result = connector
                .can_start_from((u64::from(parsed_lsn), 0))
                .unwrap();
            assert!(
                result,
                "Replication slot is created and it should be possible to continue"
            );
        })
    }

    #[test]
    #[serial]
    #[ignore]
    fn test_connector_continue_replication_from_lsn() {
        run_connector_test("postgres", |app_config| {
            let config = app_config
                .connections
                .get(0)
                .unwrap()
                .config
                .as_ref()
                .unwrap();

            let mut test_client = TestPostgresClient::new(config);
            let mut rng = rand::thread_rng();
            let table_name = format!("test_table_{}", rng.gen::<u32>());
            test_client.create_simple_table("public", &table_name);

            let tables = vec![TableInfo {
                name: table_name.clone(),
                table_name: table_name.clone(),
                id: 0,
                columns: None,
            }];

            let conn_config = map_connection_config(config).unwrap();
            let postgres_config = PostgresConfig {
                name: "test".to_string(),
                tables: Some(tables.clone()),
                config: conn_config.clone(),
            };

            let connector = PostgresConnector::new(1, postgres_config.clone());

            let mut replication_conn_config = conn_config;
            replication_conn_config.replication_mode(ReplicationMode::Logical);

            // Creating publication
            let client = helper::connect(replication_conn_config.clone()).unwrap();
            connector.create_publication(client).unwrap();

            // Creating slot
            let client = helper::connect(replication_conn_config.clone()).unwrap();
            let client_ref = Arc::new(RefCell::new(client));

            let slot_name = connector.get_slot_name();
            let parsed_lsn = create_slot(client_ref, &slot_name);

            let config = IngestionConfig::default();
            let (ingestor, mut iterator) = Ingestor::initialize_channel(config);

            thread::spawn(move || {
                let connector = PostgresConnector::new(1, postgres_config);
                let _ = connector.start(
                    Some((u64::from(parsed_lsn), 1_u64)),
                    &ingestor,
                    Some(tables),
                );
            });

            test_client.insert_rows(&table_name, 2, None);

            let mut i = 1;
            while i < 2 {
                let op = iterator.next();
                match op {
                    None => {}
                    Some(((_, seq_no), _operation)) => {
                        assert_eq!(i, seq_no);
                    }
                }
                i += 1;
            }
            assert_eq!(i, 2);

            test_client.insert_rows(&table_name, 3, None);
            while i < 5 {
                let op = iterator.next();
                match op {
                    None => {}
                    Some(((_, seq_no), _operation)) => {
                        assert_eq!(i - 2, seq_no);
                    }
                }
                i += 1;
            }

            assert_eq!(i, 5);
        })
    }
}
