#[cfg(test)]
mod tests {
    use crate::connectors::postgres::connection::helper;
    use crate::connectors::postgres::connection::helper::map_connection_config;
    use crate::connectors::postgres::connector::{PostgresConfig, PostgresConnector};
    use crate::connectors::postgres::replication_slot_helper::ReplicationSlotHelper;
    use crate::connectors::Connector;
    use crate::test_util::run_connector_test;
    use core::cell::RefCell;
    use postgres_types::PgLsn;
    use std::str::FromStr;
    use std::sync::Arc;
    use tokio_postgres::config::ReplicationMode;

    #[test]
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
            client_ref
                .borrow_mut()
                .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
                .unwrap();
            let slot_name = connector.get_slot_name();
            let created_lsn =
                ReplicationSlotHelper::create_replication_slot(client_ref.clone(), &slot_name)
                    .unwrap()
                    .unwrap();
            let parsed_lsn = PgLsn::from_str(&created_lsn).unwrap();
            client_ref.borrow_mut().simple_query("COMMIT;").unwrap();

            let result = connector
                .can_start_from((u64::from(parsed_lsn), 0))
                .unwrap();
            assert!(
                result,
                "Replication slot is created and it should be possible to continue"
            );
        })
    }
}
