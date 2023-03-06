use crate::errors::ConnectorError::UnexpectedQueryMessageError;
use crate::errors::PostgresConnectorError::FetchReplicationSlotError;
use crate::errors::{ConnectorError, PostgresConnectorError};
use dozer_types::log::debug;
use postgres::Client;
use std::cell::RefCell;
use std::sync::Arc;
use tokio_postgres::{Error, SimpleQueryMessage};

pub struct ReplicationSlotHelper {}

impl ReplicationSlotHelper {
    pub fn drop_replication_slot(
        client: Arc<RefCell<Client>>,
        slot_name: &str,
    ) -> Result<Vec<SimpleQueryMessage>, Error> {
        let res = client
            .borrow_mut()
            .simple_query(format!("select pg_drop_replication_slot('{slot_name}');").as_ref());
        match res {
            Ok(_) => debug!("dropped replication slot {}", slot_name),
            Err(_) => debug!("failed to drop replication slot..."),
        };

        res
    }

    pub fn create_replication_slot(
        client: Arc<RefCell<Client>>,
        slot_name: &str,
    ) -> Result<Option<String>, ConnectorError> {
        let create_replication_slot_query =
            format!(r#"CREATE_REPLICATION_SLOT {slot_name:?} LOGICAL "pgoutput" USE_SNAPSHOT"#);

        let slot_query_row = client
            .borrow_mut()
            .simple_query(&create_replication_slot_query)
            .map_err(|e| {
                debug!("failed to create replication slot {}", slot_name);
                ConnectorError::PostgresConnectorError(PostgresConnectorError::CreateSlotError(
                    slot_name.to_string(),
                    e,
                ))
            })?;

        if let SimpleQueryMessage::Row(row) = &slot_query_row[0] {
            Ok(row.get("consistent_point").map(|lsn| lsn.to_string()))
        } else {
            Err(UnexpectedQueryMessageError)
        }
    }

    pub fn replication_slot_exists(
        client: Arc<RefCell<Client>>,
        slot_name: &str,
    ) -> Result<bool, PostgresConnectorError> {
        let replication_slot_info_query =
            format!(r#"SELECT * FROM pg_replication_slots where slot_name = '{slot_name}';"#);

        let slot_query_row = client
            .borrow_mut()
            .simple_query(&replication_slot_info_query)
            .map_err(FetchReplicationSlotError)?;

        Ok(matches!(
            slot_query_row.get(0),
            Some(SimpleQueryMessage::Row(_))
        ))
    }
}

#[cfg(test)]
mod tests {
    use postgres::NoTls;
    use serial_test::serial;
    use std::cell::RefCell;
    use std::sync::Arc;
    use tokio_postgres::config::ReplicationMode;

    use crate::errors::{ConnectorError, PostgresConnectorError};
    use crate::test_util::{get_config, run_connector_test};

    use super::ReplicationSlotHelper;

    #[test]
    #[serial]
    fn test_connector_replication_slot_create_successfully() {
        run_connector_test("postgres", |app_config| {
            let mut config = get_config(app_config);
            config.replication_mode(ReplicationMode::Logical);

            let client = postgres::Config::from(config.clone())
                .connect(NoTls)
                .unwrap();

            let client_ref = Arc::new(RefCell::new(client));

            client_ref
                .borrow_mut()
                .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
                .unwrap();

            let actual = ReplicationSlotHelper::create_replication_slot(client_ref, "test");

            assert!(actual.is_ok());

            match actual {
                Err(_) => panic!("Validation should fail"),
                Ok(result) => {
                    if let Some(address) = result {
                        assert_ne!(address, "")
                    } else {
                        panic!("Validation should fail")
                    }
                }
            }
        });
    }

    #[test]
    #[serial]
    fn test_connector_replication_slot_create_failed_if_existed() {
        run_connector_test("postgres", |app_config| {
            let slot_name = "test";
            let mut config = get_config(app_config);
            config.replication_mode(ReplicationMode::Logical);

            let client = postgres::Config::from(config.clone())
                .connect(NoTls)
                .unwrap();

            let client_ref = Arc::new(RefCell::new(client));

            client_ref
                .borrow_mut()
                .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
                .unwrap();

            let create_replication_slot_query = format!(
                r#"CREATE_REPLICATION_SLOT {:?} LOGICAL "pgoutput" USE_SNAPSHOT"#,
                slot_name
            );

            client_ref
                .borrow_mut()
                .simple_query(&create_replication_slot_query)
                .expect("failed");

            let actual =
                ReplicationSlotHelper::create_replication_slot(client_ref, slot_name);

            assert!(actual.is_err());

            match actual {
                Ok(_) => panic!("Validation should fail"),
                Err(e) => {
                    assert!(matches!(e, ConnectorError::PostgresConnectorError(_)));

                    if let ConnectorError::PostgresConnectorError(cnn_err) = e {
                        if let PostgresConnectorError::CreateSlotError(_, err) = cnn_err {
                            assert_eq!(
                                err.as_db_error().unwrap().message(),
                                format!("replication slot \"{slot_name}\" already exists")
                            );
                        } else {
                            panic!("Unexpected error occurred");
                        }
                    } else {
                        panic!("Unexpected error occurred");
                    }
                }
            }
        });
    }

    #[test]
    #[serial]
    fn test_connector_replication_slot_drop_successfully() {
        run_connector_test("postgres", |app_config| {
            let slot_name = "test";
            let mut config = get_config(app_config);
            config.replication_mode(ReplicationMode::Logical);

            let client = postgres::Config::from(config.clone())
                .connect(NoTls)
                .unwrap();

            let client_ref = Arc::new(RefCell::new(client));

            client_ref
                .borrow_mut()
                .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
                .unwrap();

            let create_replication_slot_query = format!(
                r#"CREATE_REPLICATION_SLOT {:?} LOGICAL "pgoutput" USE_SNAPSHOT"#,
                slot_name
            );

            client_ref
                .borrow_mut()
                .simple_query(&create_replication_slot_query)
                .expect("failed");

            let actual =
                ReplicationSlotHelper::drop_replication_slot(client_ref, slot_name);

            assert!(actual.is_ok());
        });
    }

    #[test]
    #[serial]
    fn test_connector_replication_slot_drop_failed_if_slot_not_exist() {
        run_connector_test("postgres", |app_config| {
            let slot_name = "test";
            let mut config = get_config(app_config);
            config.replication_mode(ReplicationMode::Logical);

            let client = postgres::Config::from(config.clone())
                .connect(NoTls)
                .unwrap();

            let client_ref = Arc::new(RefCell::new(client));

            client_ref
                .borrow_mut()
                .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
                .unwrap();

            let actual =
                ReplicationSlotHelper::drop_replication_slot(client_ref, slot_name);

            assert!(actual.is_err());

            match actual {
                Ok(_) => panic!("Validation should fail"),
                Err(e) => {
                    assert_eq!(
                        e.as_db_error().unwrap().message(),
                        format!("replication slot \"{slot_name}\" does not exist")
                    );
                }
            }
        });
    }
}
