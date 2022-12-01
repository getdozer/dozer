use crate::connectors::postgres::connector::ReplicationSlotInfo;

use crate::connectors::TableInfo;
use crate::errors::ConnectorError;
use crate::errors::ConnectorError::InvalidQueryError;
use crate::errors::PostgresConnectorError;
use postgres::Client;
use postgres_types::PgLsn;
use std::borrow::BorrowMut;
use std::collections::HashMap;

pub fn validate_connection(
    config: tokio_postgres::Config,
    tables: Option<Vec<TableInfo>>,
    replication_info: Option<ReplicationSlotInfo>,
) -> Result<(), ConnectorError> {
    let mut client = super::helper::connect(config)?;

    validate_details(client.borrow_mut())?;
    validate_user(client.borrow_mut())?;

    if let Some(tables_info) = tables {
        validate_tables(client.borrow_mut(), tables_info)?;
    }

    validate_wal_level(client.borrow_mut())?;

    if let Some(replication_details) = replication_info {
        validate_slot(client.borrow_mut(), replication_details)?;
    } else {
        validate_limit_of_replications(client.borrow_mut())?;
    }

    Ok(())
}

fn validate_details(client: &mut Client) -> Result<(), ConnectorError> {
    client
        .simple_query("SELECT version()")
        .map_err(|e| PostgresConnectorError::ConnectToDatabaseError(e.to_string()))?;

    Ok(())
}

fn validate_user(client: &mut Client) -> Result<(), ConnectorError> {
    client
        .query_one(
            "SELECT * FROM pg_user WHERE usename = \"current_user\"() AND userepl = true",
            &[],
        )
        .map_err(|_e| PostgresConnectorError::ReplicationIsNotAvailableForUserError)?;

    Ok(())
}

fn validate_wal_level(client: &mut Client) -> Result<(), ConnectorError> {
    let result = client
        .query_one("SHOW wal_level", &[])
        .map_err(|_e| PostgresConnectorError::WALLevelIsNotCorrect())?;

    let wal_level: Result<String, _> = result.try_get(0);

    match wal_level {
        Ok(level) => {
            if level == "logical" {
                Ok(())
            } else {
                Err(ConnectorError::PostgresConnectorError(
                    PostgresConnectorError::WALLevelIsNotCorrect(),
                ))
            }
        }
        Err(e) => Err(ConnectorError::InternalError(Box::new(e))),
    }
}

fn validate_tables(client: &mut Client, table_info: Vec<TableInfo>) -> Result<(), ConnectorError> {
    let mut tables_names: HashMap<String, bool> = HashMap::new();
    table_info.iter().for_each(|t| {
        tables_names.insert(t.name.clone(), true);
    });

    let table_name_keys: Vec<String> = tables_names.keys().cloned().collect();
    let result = client
        .query(
            "SELECT table_name FROM information_schema.tables WHERE table_name = ANY($1)",
            &[&table_name_keys],
        )
        .map_err(|_e| PostgresConnectorError::TableError(table_name_keys))?;

    for r in result.iter() {
        let table_name: String = r.try_get(0).map_err(InvalidQueryError)?;
        tables_names.remove(&table_name);
    }

    if !tables_names.is_empty() {
        let table_name_keys = tables_names.keys().cloned().collect();
        Err(ConnectorError::PostgresConnectorError(
            PostgresConnectorError::TableError(table_name_keys),
        ))
    } else {
        Ok(())
    }
}

fn validate_slot(
    client: &mut Client,
    replication_info: ReplicationSlotInfo,
) -> Result<(), ConnectorError> {
    let result = client
        .query_one(
            "SELECT active, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
            &[&replication_info.name],
        )
        .map_err(|_e| PostgresConnectorError::SlotNotExistError(replication_info.name.clone()))?;

    let is_already_running: bool = result.try_get(0).map_err(InvalidQueryError)?;
    if is_already_running {
        return Err(ConnectorError::PostgresConnectorError(
            PostgresConnectorError::SlotIsInUseError(replication_info.name.clone()),
        ));
    }

    let flush_lsn: PgLsn = result.try_get(1).map_err(InvalidQueryError)?;

    if flush_lsn.gt(&replication_info.start_lsn) {
        Err(ConnectorError::PostgresConnectorError(
            PostgresConnectorError::StartLsnIsBeforeLastFlushedLsnError(
                flush_lsn.to_string(),
                replication_info.start_lsn.to_string(),
            ),
        ))
    } else {
        Ok(())
    }
}

fn validate_limit_of_replications(client: &mut Client) -> Result<(), ConnectorError> {
    let slots_limit_result = client
        .query_one("SHOW max_replication_slots", &[])
        .map_err(|e| PostgresConnectorError::ConnectToDatabaseError(e.to_string()))?;

    let slots_limit_str: String = slots_limit_result.try_get(0).map_err(InvalidQueryError)?;
    let slots_limit: i64 = slots_limit_str.parse().unwrap();

    let used_slots_result = client
        .query_one("SELECT COUNT(*) FROM pg_replication_slots;", &[])
        .map_err(|e| PostgresConnectorError::ConnectToDatabaseError(e.to_string()))?;

    let used_slots: i64 = used_slots_result.try_get(0).map_err(InvalidQueryError)?;

    if used_slots == slots_limit {
        Err(ConnectorError::PostgresConnectorError(
            PostgresConnectorError::NoAvailableSlotsError,
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::connectors::postgres::connection::validator::validate_connection;
    use crate::connectors::postgres::connector::ReplicationSlotInfo;

    use postgres_types::PgLsn;

    use std::ops::Deref;
    use std::panic;

    use tokio_postgres::NoTls;

    use crate::connectors::TableInfo;
    use crate::errors::{ConnectorError, PostgresConnectorError};
    use serial_test::serial;

    fn get_config() -> tokio_postgres::Config {
        let mut config = tokio_postgres::Config::new();
        config
            .dbname("users")
            .user("postgres")
            .host("localhost")
            .deref()
            .clone()
    }

    #[test]
    #[ignore]
    #[serial]
    fn test_fail_to_connect() {
        run_test(|| {
            let mut config = get_config();
            config.dbname("not_existing");

            let result = validate_connection(config, None, None);
            assert!(result.is_err());
        });
    }

    #[test]
    #[ignore]
    #[serial]
    fn test_user_not_have_permission_to_use_replication() {
        run_test(|| {
            let mut config = get_config();
            let mut client = postgres::Config::from(config.clone())
                .connect(NoTls)
                .unwrap();

            client
                .simple_query("CREATE USER dozer_test_without_permission")
                .expect("User creation failed");
            config.user("dozer_test_without_permission");

            let result = validate_connection(config, None, None);

            client
                .simple_query("DROP USER dozer_test_without_permission")
                .expect("User delete failed");

            assert!(result.is_err());

            match result {
                Ok(_) => panic!("Validation should fail"),
                Err(ConnectorError::PostgresConnectorError(e)) => {
                    assert_eq!(
                        e,
                        PostgresConnectorError::ReplicationIsNotAvailableForUserError
                    );
                }
                Err(_) => panic!("Unexpected error occurred"),
            }
        });
    }

    #[test]
    #[ignore]
    #[serial]
    fn test_requested_tables_not_exist() {
        run_test(|| {
            let config = get_config();
            let mut client = postgres::Config::from(config.clone())
                .connect(NoTls)
                .unwrap();

            client
                .simple_query("DROP TABLE IF EXISTS not_existing")
                .expect("User creation failed");

            let tables = vec![TableInfo {
                name: "not_existing".to_string(),
                id: 0,
                columns: None,
            }];
            let result = validate_connection(config, Some(tables), None);

            assert!(result.is_err());

            match result {
                Ok(_) => panic!("Validation should fail"),
                Err(ConnectorError::PostgresConnectorError(e)) => {
                    assert_eq!(
                        e,
                        PostgresConnectorError::TableError(vec!["not_existing".to_string()])
                    );
                }
                Err(_) => panic!("Unexpected error occurred"),
            }
        });
    }

    #[test]
    #[ignore]
    #[serial]
    fn test_replication_slot_not_exist() {
        let config = get_config();
        let _client = postgres::Config::from(config.clone())
            .connect(NoTls)
            .unwrap();

        let replication_info = ReplicationSlotInfo {
            name: "not_existing_slot".to_string(),
            start_lsn: PgLsn::from(0),
        };
        let result = validate_connection(config, None, Some(replication_info));

        assert!(result.is_err());

        match result {
            Ok(_) => panic!("Validation should fail"),
            Err(ConnectorError::PostgresConnectorError(e)) => {
                assert_eq!(
                    e,
                    PostgresConnectorError::SlotNotExistError("not_existing_slot".to_string())
                );
            }
            Err(_) => panic!("Unexpected error occurred"),
        }
    }

    #[test]
    #[ignore]
    #[serial]
    fn test_start_lsn_is_before_last_flush_lsn() {
        let config = get_config();
        let mut client = postgres::Config::from(config.clone())
            .connect(NoTls)
            .unwrap();

        client
            .query(
                r#"SELECT pg_create_logical_replication_slot('existing_slot', 'pgoutput');"#,
                &[],
            )
            .expect("User creation failed");

        let replication_info = ReplicationSlotInfo {
            name: "existing_slot".to_string(),
            start_lsn: PgLsn::from(0),
        };
        let result = validate_connection(config, None, Some(replication_info));

        client
            .query(r#"SELECT pg_drop_replication_slot('existing_slot');"#, &[])
            .expect("Slot drop failed");

        assert!(result.is_err());

        match result {
            Ok(_) => panic!("Validation should fail"),
            Err(ConnectorError::PostgresConnectorError(
                PostgresConnectorError::StartLsnIsBeforeLastFlushedLsnError(_, _),
            )) => {}
            Err(_) => panic!("Unexpected error occurred"),
        }
    }

    #[test]
    #[ignore]
    #[serial]
    fn test_limit_of_replication_slots_reached() {
        let config = get_config();
        let mut client = postgres::Config::from(config.clone())
            .connect(NoTls)
            .unwrap();

        let slots_limit_result = client.query_one("SHOW max_replication_slots", &[]).unwrap();

        let slots_limit_str: String = slots_limit_result.try_get(0).unwrap();
        let slots_limit: i64 = slots_limit_str.parse().unwrap();

        let used_slots_result = client
            .query_one("SELECT COUNT(*) FROM pg_replication_slots;", &[])
            .unwrap();

        let used_slots: i64 = used_slots_result.try_get(0).unwrap();

        let range = used_slots..slots_limit - 1;
        for n in range {
            let slot_name = format!("slot_{}", n);
            client
                .query(
                    r#"SELECT pg_create_logical_replication_slot($1, 'pgoutput');"#,
                    &[&slot_name],
                )
                .unwrap();
        }

        // One replication slot is available
        let result = validate_connection(config.clone(), None, None);
        assert!(result.is_ok());

        let slot_name = format!("slot_{}", slots_limit - 1);
        client
            .query(
                r#"SELECT pg_create_logical_replication_slot($1, 'pgoutput');"#,
                &[&slot_name],
            )
            .unwrap();

        // No replication slots are available
        let result = validate_connection(config, None, None);
        assert!(result.is_err());

        match result.unwrap_err() {
            ConnectorError::PostgresConnectorError(
                PostgresConnectorError::NoAvailableSlotsError,
            ) => {}
            _ => panic!("Unexpected error occurred"),
        }

        // Teardown
        for n in used_slots..slots_limit {
            let slot_name = format!("slot_{}", n);
            client
                .query(r#"SELECT pg_drop_replication_slot($1);"#, &[&slot_name])
                .expect("Slot drop failed");
        }
    }

    fn setup() {
        let config = postgres::Config::from(get_config());
        let mut client = config.connect(NoTls).unwrap();
        client
            .query("DROP DATABASE IF EXISTS dozer_tests", &[])
            .unwrap();
        client.query("CREATE DATABASE dozer_tests", &[]).unwrap();
    }

    fn run_test<T>(test: T)
    where
        T: FnOnce() + panic::UnwindSafe,
    {
        setup();

        let result = panic::catch_unwind(|| {
            test();
        });

        assert!(result.is_ok())
    }
}
