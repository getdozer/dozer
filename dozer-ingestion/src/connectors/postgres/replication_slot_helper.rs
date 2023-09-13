use super::connection::client::Client;
use crate::errors::ConnectorError::UnexpectedQueryMessageError;
use crate::errors::PostgresConnectorError::{FetchReplicationSlotError, InvalidQueryError};
use crate::errors::{ConnectorError, PostgresConnectorError};
use dozer_types::log::debug;
use tokio_postgres::{Error, SimpleQueryMessage};

pub struct ReplicationSlotHelper {}

impl ReplicationSlotHelper {
    pub async fn drop_replication_slot(
        client: &mut Client,
        slot_name: &str,
    ) -> Result<Vec<SimpleQueryMessage>, Error> {
        let res = client
            .simple_query(format!("select pg_drop_replication_slot('{slot_name}');").as_ref())
            .await;
        match res {
            Ok(_) => debug!("dropped replication slot {}", slot_name),
            Err(_) => debug!("failed to drop replication slot..."),
        };

        res
    }

    pub async fn create_replication_slot(
        client: &mut Client,
        slot_name: &str,
    ) -> Result<Option<String>, ConnectorError> {
        let create_replication_slot_query =
            format!(r#"CREATE_REPLICATION_SLOT {slot_name:?} LOGICAL "pgoutput" USE_SNAPSHOT"#);

        let slot_query_row = client
            .simple_query(&create_replication_slot_query)
            .await
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

    pub async fn replication_slot_exists(
        client: &mut Client,
        slot_name: &str,
    ) -> Result<bool, PostgresConnectorError> {
        let replication_slot_info_query =
            format!(r#"SELECT * FROM pg_replication_slots where slot_name = '{slot_name}';"#);

        let slot_query_row = client
            .simple_query(&replication_slot_info_query)
            .await
            .map_err(FetchReplicationSlotError)?;

        Ok(matches!(
            slot_query_row.get(0),
            Some(SimpleQueryMessage::Row(_))
        ))
    }

    pub async fn clear_inactive_slots(
        client: &mut Client,
        slot_name_prefix: &str,
    ) -> Result<(), PostgresConnectorError> {
        let inactive_slots_query = format!(
            r#"SELECT * FROM pg_replication_slots where active = false AND slot_name LIKE '{slot_name_prefix}%';"#
        );

        let slots = client
            .simple_query(&inactive_slots_query)
            .await
            .map_err(FetchReplicationSlotError)?;

        let column_index = if let Some(SimpleQueryMessage::Row(row)) = slots.get(0) {
            row.columns().iter().position(|c| c.name() == "slot_name")
        } else {
            None
        };

        for slot_message in slots {
            if let SimpleQueryMessage::Row(row) = slot_message {
                if let Some(index) = column_index {
                    let slot_name = row.get(index);

                    if let Some(name) = slot_name {
                        Self::drop_replication_slot(client, name)
                            .await
                            .map_err(InvalidQueryError)?;
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;
    use tokio_postgres::config::ReplicationMode;

    use crate::connectors::postgres::connection::helper::connect;
    use crate::connectors::postgres::test_utils::get_config;
    use crate::errors::{ConnectorError, PostgresConnectorError};
    use crate::test_util::run_connector_test;

    use super::ReplicationSlotHelper;

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_replication_slot_create_successfully() {
        run_connector_test("postgres", |app_config| async move {
            let mut config = get_config(app_config);
            config.replication_mode(ReplicationMode::Logical);

            let mut client = connect(config).await.unwrap();

            client
                .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
                .await
                .unwrap();

            let actual = ReplicationSlotHelper::create_replication_slot(&mut client, "test").await;

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
        })
        .await
    }

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_replication_slot_create_failed_if_existed() {
        run_connector_test("postgres", |app_config| async move {
            let slot_name = "test";
            let mut config = get_config(app_config);
            config.replication_mode(ReplicationMode::Logical);

            let mut client = connect(config).await.unwrap();

            client
                .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
                .await
                .unwrap();

            let create_replication_slot_query =
                format!(r#"CREATE_REPLICATION_SLOT {slot_name:?} LOGICAL "pgoutput" USE_SNAPSHOT"#);

            client
                .simple_query(&create_replication_slot_query)
                .await
                .expect("failed");

            let actual =
                ReplicationSlotHelper::create_replication_slot(&mut client, slot_name).await;

            assert!(actual.is_err());

            match actual {
                Ok(_) => panic!("Validation should fail"),
                Err(e) => {
                    assert!(matches!(e, ConnectorError::PostgresConnectorError(_)));

                    if let ConnectorError::PostgresConnectorError(
                        PostgresConnectorError::CreateSlotError(_, err),
                    ) = e
                    {
                        assert_eq!(
                            err.as_db_error().unwrap().message(),
                            format!("replication slot \"{slot_name}\" already exists")
                        );
                    } else {
                        panic!("Unexpected error occurred");
                    }
                }
            }
        })
        .await
    }

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_replication_slot_drop_successfully() {
        run_connector_test("postgres", |app_config| async move {
            let slot_name = "test";
            let mut config = get_config(app_config);
            config.replication_mode(ReplicationMode::Logical);

            let mut client = connect(config).await.unwrap();

            client
                .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
                .await
                .unwrap();

            let create_replication_slot_query =
                format!(r#"CREATE_REPLICATION_SLOT {slot_name:?} LOGICAL "pgoutput" USE_SNAPSHOT"#);

            client
                .simple_query(&create_replication_slot_query)
                .await
                .expect("failed");

            let actual = ReplicationSlotHelper::drop_replication_slot(&mut client, slot_name).await;

            assert!(actual.is_ok());
        })
        .await
    }

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_replication_slot_drop_failed_if_slot_not_exist() {
        run_connector_test("postgres", |app_config| async move {
            let slot_name = "test";
            let mut config = get_config(app_config);
            config.replication_mode(ReplicationMode::Logical);

            let mut client = connect(config).await.unwrap();

            client
                .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
                .await
                .unwrap();

            let actual = ReplicationSlotHelper::drop_replication_slot(&mut client, slot_name).await;

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
        })
        .await
    }
}
