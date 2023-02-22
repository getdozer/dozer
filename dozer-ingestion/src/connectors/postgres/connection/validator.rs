use crate::connectors::postgres::connector::ReplicationSlotInfo;

use crate::connectors::TableInfo;
use crate::errors::PostgresConnectorError;
use crate::errors::PostgresConnectorError::{
    ColumnNameNotValid, ConnectionFailure, InvalidQueryError, MissingTableInReplicationSlot,
    NoAvailableSlotsError, ReplicationIsNotAvailableForUserError, SlotIsInUseError,
    SlotNotExistError, StartLsnIsBeforeLastFlushedLsnError, TableError, TableNameNotValid,
    WALLevelIsNotCorrect,
};
use dozer_types::indicatif::ProgressStyle;
use postgres::Client;
use postgres_types::PgLsn;
use regex::Regex;
use std::borrow::BorrowMut;
use std::collections::HashMap;

pub enum Validations {
    Details,
    User,
    Tables,
    WALLevel,
    Slot,
}

pub fn validate_connection(
    name: &str,
    config: tokio_postgres::Config,
    tables: Option<&Vec<TableInfo>>,
    replication_info: Option<ReplicationSlotInfo>,
) -> Result<(), PostgresConnectorError> {
    let validations_order: Vec<Validations> = vec![
        Validations::Details,
        Validations::User,
        Validations::Tables,
        Validations::WALLevel,
        Validations::Slot,
    ];
    let pb = dozer_types::indicatif::ProgressBar::new(validations_order.len() as u64);
    pb.set_style(
        ProgressStyle::with_template(&format!(
            "[{}] {}",
            name, "{spinner:.green} {wide_msg} {bar}"
        ))
        .unwrap(),
    );
    pb.set_message("Validating connection to source");

    let mut client = super::helper::connect(config)?;

    for validation_type in validations_order {
        match validation_type {
            Validations::Details => validate_details(client.borrow_mut())?,
            Validations::User => validate_user(client.borrow_mut())?,
            Validations::Tables => {
                if let Some(tables_info) = &tables {
                    validate_tables(client.borrow_mut(), tables_info)?;
                }
            }
            Validations::WALLevel => validate_wal_level(client.borrow_mut())?,
            Validations::Slot => {
                if let Some(replication_details) = &replication_info {
                    validate_slot(client.borrow_mut(), replication_details, tables)?;
                } else {
                    validate_limit_of_replications(client.borrow_mut())?;
                }
            }
        }

        pb.inc(1);
    }

    pb.finish_and_clear();

    Ok(())
}

fn validate_details(client: &mut Client) -> Result<(), PostgresConnectorError> {
    client
        .simple_query("SELECT version()")
        .map_err(ConnectionFailure)?;

    Ok(())
}

fn validate_user(client: &mut Client) -> Result<(), PostgresConnectorError> {
    client
        .query_one(
            "
                SELECT r.rolcanlogin AS can_login, r.rolreplication AS is_replication_role,
                    ARRAY(SELECT b.rolname
                             FROM pg_catalog.pg_auth_members m
                                      JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)
                             WHERE m.member = r.oid
                    ) &&
                      '{rds_superuser, rdsadmin, rdsrepladmin, rds_replication}'::name[] AS is_aws_replication_role
                FROM pg_roles r
                WHERE r.rolname = current_user
            ",
            &[],
        )
        .map_or(Err(ReplicationIsNotAvailableForUserError), |row| {
            let can_login: bool = row.get("can_login");
            let is_replication_role: bool = row.get("is_replication_role");
            let is_aws_replication_role: bool = row.get("is_aws_replication_role");

            if can_login && (is_replication_role || is_aws_replication_role) {
                Ok(())
            } else {
                Err(ReplicationIsNotAvailableForUserError)
            }
        })
}

fn validate_wal_level(client: &mut Client) -> Result<(), PostgresConnectorError> {
    let result = client
        .query_one("SHOW wal_level", &[])
        .map_err(|_e| WALLevelIsNotCorrect())?;

    let wal_level: Result<String, _> = result.try_get(0);
    wal_level.map_or_else(
        |e| Err(InvalidQueryError(e)),
        |level| {
            if level == "logical" {
                Ok(())
            } else {
                Err(WALLevelIsNotCorrect())
            }
        },
    )
}

fn validate_tables_names(table_info: &Vec<TableInfo>) -> Result<(), PostgresConnectorError> {
    let table_regex = Regex::new(r"^([[:lower:]_][[:alnum:]_]*)$").unwrap();
    for t in table_info {
        if !table_regex.is_match(&t.table_name) {
            return Err(TableNameNotValid(t.table_name.clone()));
        }
    }

    Ok(())
}

fn validate_columns_names(table_info: &Vec<TableInfo>) -> Result<(), PostgresConnectorError> {
    let column_name_regex = Regex::new(r"^([[:lower:]_][[:alnum:]_]*)$").unwrap();
    for t in table_info {
        if let Some(columns) = &t.columns {
            for column in columns {
                if !column_name_regex.is_match(&column.name) {
                    return Err(ColumnNameNotValid(column.name.clone()));
                }
            }
        }
    }

    Ok(())
}

fn validate_tables(
    client: &mut Client,
    table_info: &Vec<TableInfo>,
) -> Result<(), PostgresConnectorError> {
    let mut tables_names: HashMap<String, bool> = HashMap::new();
    table_info.iter().for_each(|t| {
        tables_names.insert(t.table_name.clone(), true);
    });

    validate_tables_names(table_info)?;
    validate_columns_names(table_info)?;

    let table_name_keys: Vec<String> = tables_names.keys().cloned().collect();
    let result = client
        .query(
            "SELECT table_name FROM information_schema.tables WHERE table_name = ANY($1)",
            &[&table_name_keys],
        )
        .map_err(InvalidQueryError)?;

    for r in result.iter() {
        let table_name: String = r.try_get(0).map_err(InvalidQueryError)?;
        tables_names.remove(&table_name);
    }

    if !tables_names.is_empty() {
        let table_name_keys = tables_names.keys().cloned().collect();
        Err(TableError(table_name_keys))
    } else {
        Ok(())
    }
}

pub fn validate_slot(
    client: &mut Client,
    replication_info: &ReplicationSlotInfo,
    tables: Option<&Vec<TableInfo>>,
) -> Result<(), PostgresConnectorError> {
    let result = client
        .query_one(
            "SELECT active, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
            &[&replication_info.name],
        )
        .map_err(InvalidQueryError)?;

    let is_already_running: bool = result.try_get(0).map_err(InvalidQueryError)?;
    if is_already_running {
        return Err(SlotIsInUseError(replication_info.name.clone()));
    }

    let flush_lsn: PgLsn = result.try_get(1).map_err(InvalidQueryError)?;

    if flush_lsn.gt(&replication_info.start_lsn) {
        return Err(StartLsnIsBeforeLastFlushedLsnError(
            flush_lsn.to_string(),
            replication_info.start_lsn.to_string(),
        ));
    }

    if let Some(tables_list) = tables {
        let result = client
            .query(
                "SELECT pc.relname FROM pg_publication pb
                    LEFT OUTER JOIN pg_publication_rel pbl on pb.oid = pbl.prpubid
                    LEFT OUTER JOIN pg_class pc on pc.oid = pbl.prrelid
                WHERE pubname = $1",
                &[&replication_info.name],
            )
            .map_err(|_e| SlotNotExistError(replication_info.name.clone()))?;

        let mut publication_tables: Vec<String> = vec![];
        for row in result {
            publication_tables.push(row.get(0));
        }

        for t in tables_list {
            if !publication_tables.contains(&t.table_name) {
                return Err(MissingTableInReplicationSlot(t.table_name.clone()));
            }
        }
    }

    Ok(())
}

fn validate_limit_of_replications(client: &mut Client) -> Result<(), PostgresConnectorError> {
    let slots_limit_result = client
        .query_one("SHOW max_replication_slots", &[])
        .map_err(ConnectionFailure)?;

    let slots_limit_str: String = slots_limit_result.try_get(0).map_err(InvalidQueryError)?;
    let slots_limit: i64 = slots_limit_str.parse().unwrap();

    let used_slots_result = client
        .query_one("SELECT COUNT(*) FROM pg_replication_slots;", &[])
        .map_err(ConnectionFailure)?;

    let used_slots: i64 = used_slots_result.try_get(0).map_err(InvalidQueryError)?;

    if used_slots == slots_limit {
        Err(NoAvailableSlotsError)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::connectors::postgres::connection::validator::{
        validate_columns_names, validate_connection, validate_tables_names,
    };
    use crate::test_util::run_connector_test;
    // use crate::connectors::postgres::connector::ReplicationSlotInfo;

    // use postgres_types::PgLsn;

    use std::ops::Deref;
    use std::panic;

    use tokio_postgres::NoTls;

    use crate::connectors::{ColumnInfo, TableInfo};
    use crate::errors::PostgresConnectorError;
    use dozer_types::models::app_config::Config;
    use dozer_types::models::connection::ConnectionConfig;
    use serial_test::serial;

    fn get_config(app_config: Config) -> tokio_postgres::Config {
        if let Some(ConnectionConfig::Postgres(connection)) =
            &app_config.connections.get(0).unwrap().config
        {
            let mut config = tokio_postgres::Config::new();
            config
                .dbname(&connection.database)
                .user(&connection.user)
                .host(&connection.host)
                .port(connection.port as u16)
                .deref()
                .clone()
        } else {
            panic!("Postgres config was expected")
        }
    }

    #[test]
    #[ignore]
    #[serial]
    fn test_fail_to_connect() {
        // run_test(|| {
        // let mut config = get_config();
        // config.dbname("not_existing");
        //
        // let result = validate_connection("pg_test_conn", config, None, None);
        // assert!(result.is_err());
        // });
    }

    #[test]
    #[ignore]
    #[serial]
    fn test_user_not_have_permission_to_use_replication() {
        // run_test(|| {
        // let mut config = get_config();
        // let mut client = postgres::Config::from(config.clone())
        //     .connect(NoTls)
        //     .unwrap();
        //
        // client
        //     .simple_query("CREATE USER dozer_test_without_permission")
        //     .expect("User creation failed");
        // config.user("dozer_test_without_permission");
        //
        // let result = validate_connection("pg_test_conn", config, None, None);
        //
        // client
        //     .simple_query("DROP USER dozer_test_without_permission")
        //     .expect("User delete failed");
        //
        // assert!(result.is_err());
        //
        // match result {
        //     Ok(_) => panic!("Validation should fail"),
        //     Err(e) => {
        //         assert!(matches!(
        //             e,
        //             PostgresConnectorError::ReplicationIsNotAvailableForUserError
        //         ));
        //     }
        // }
        // });
    }

    #[test]
    #[ignore]
    #[serial]
    fn test_requested_tables_not_exist() {
        run_connector_test("postgres", |app_config| {
            let config = get_config(app_config);
            let mut client = postgres::Config::from(config.clone())
                .connect(NoTls)
                .unwrap();

            client
                .simple_query("DROP TABLE IF EXISTS not_existing")
                .expect("User creation failed");

            let tables = vec![TableInfo {
                name: "not_existing".to_string(),
                table_name: "not_existing".to_string(),
                id: 0,
                columns: None,
            }];
            let result = validate_connection("pg_test_conn", config, Some(&tables), None);

            assert!(result.is_err());

            match result {
                Ok(_) => panic!("Validation should fail"),
                Err(e) => {
                    assert!(matches!(e, PostgresConnectorError::TableError(_)));

                    if let PostgresConnectorError::TableError(msg) = e {
                        assert_eq!(msg, vec!["not_existing".to_string()]);
                    } else {
                        panic!("Unexpected error occurred");
                    }
                }
            }
        });
    }

    #[test]
    #[ignore]
    #[serial]
    fn test_replication_slot_not_exist() {
        // let config = get_config();
        // let _client = postgres::Config::from(config.clone())
        //     .connect(NoTls)
        //     .unwrap();
        //
        // let replication_info = ReplicationSlotInfo {
        //     name: "not_existing_slot".to_string(),
        //     start_lsn: PgLsn::from(0),
        // };
        // let result = validate_connection("pg_test_conn", config, None, Some(replication_info));
        //
        // assert!(result.is_err());
        //
        // match result {
        //     Ok(_) => panic!("Validation should fail"),
        //     Err(e) => {
        //         assert!(matches!(e, PostgresConnectorError::SlotNotExistError(_)));
        //
        //         if let PostgresConnectorError::SlotNotExistError(msg) = e {
        //             assert_eq!(msg, "not_existing_slot");
        //         } else {
        //             panic!("Unexpected error occurred");
        //         }
        //     }
        // }
    }

    #[test]
    #[ignore]
    #[serial]
    fn test_start_lsn_is_before_last_flush_lsn() {
        // let config = get_config();
        // let mut client = postgres::Config::from(config.clone())
        //     .connect(NoTls)
        //     .unwrap();
        //
        // client
        //     .query(
        //         r#"SELECT pg_create_logical_replication_slot('existing_slot', 'pgoutput');"#,
        //         &[],
        //     )
        //     .expect("User creation failed");
        //
        // let replication_info = ReplicationSlotInfo {
        //     name: "existing_slot".to_string(),
        //     start_lsn: PgLsn::from(0),
        // };
        // let result = validate_connection("pg_test_conn", config, None, Some(replication_info));
        //
        // client
        //     .query(r#"SELECT pg_drop_replication_slot('existing_slot');"#, &[])
        //     .expect("Slot drop failed");
        //
        // assert!(result.is_err());
        //
        // match result {
        //     Ok(_) => panic!("Validation should fail"),
        //     Err(PostgresConnectorError::StartLsnIsBeforeLastFlushedLsnError(_, _)) => {}
        //     Err(_) => panic!("Unexpected error occurred"),
        // }
    }

    #[test]
    #[ignore]
    #[serial]
    fn test_limit_of_replication_slots_reached() {
        // let config = get_config();
        // let mut client = postgres::Config::from(config.clone())
        //     .connect(NoTls)
        //     .unwrap();
        //
        // let slots_limit_result = client.query_one("SHOW max_replication_slots", &[]).unwrap();
        //
        // let slots_limit_str: String = slots_limit_result.try_get(0).unwrap();
        // let slots_limit: i64 = slots_limit_str.parse().unwrap();
        //
        // let used_slots_result = client
        //     .query_one("SELECT COUNT(*) FROM pg_replication_slots;", &[])
        //     .unwrap();
        //
        // let used_slots: i64 = used_slots_result.try_get(0).unwrap();
        //
        // let range = used_slots..slots_limit - 1;
        // for n in range {
        //     let slot_name = format!("slot_{n}");
        //     client
        //         .query(
        //             r#"SELECT pg_create_logical_replication_slot($1, 'pgoutput');"#,
        //             &[&slot_name],
        //         )
        //         .unwrap();
        // }
        //
        // // One replication slot is available
        // let result = validate_connection("pg_test_conn", config.clone(), None, None);
        // assert!(result.is_ok());
        //
        // let slot_name = format!("slot_{}", slots_limit - 1);
        // client
        //     .query(
        //         r#"SELECT pg_create_logical_replication_slot($1, 'pgoutput');"#,
        //         &[&slot_name],
        //     )
        //     .unwrap();
        //
        // // No replication slots are available
        // let result = validate_connection("pg_test_conn", config, None, None);
        // assert!(result.is_err());
        //
        // match result.unwrap_err() {
        //     PostgresConnectorError::NoAvailableSlotsError => {}
        //     _ => panic!("Unexpected error occurred"),
        // }
        //
        // // Teardown
        // for n in used_slots..slots_limit {
        //     let slot_name = format!("slot_{n}");
        //     client
        //         .query(r#"SELECT pg_drop_replication_slot($1);"#, &[&slot_name])
        //         .expect("Slot drop failed");
        // }
    }

    #[test]
    fn test_validate_tables_names() {
        let tables_with_result = vec![
            ("test", true),
            ("Test", false),
            (";Drop table test", false),
            ("test_with_underscore", true),
        ];

        for (table_name, expected_result) in tables_with_result {
            let res = validate_tables_names(&vec![TableInfo {
                name: table_name.to_string(),
                table_name: table_name.to_string(),
                id: 0,
                columns: None,
            }]);

            assert_eq!(expected_result, res.is_ok());
        }
    }

    #[test]
    fn test_validate_column_names() {
        let columns_names_with_result = vec![
            ("test", true),
            ("Test", false),
            (";Drop table test", false),
            ("test_with_underscore", true),
        ];

        for (column_name, expected_result) in columns_names_with_result {
            let res = validate_columns_names(&vec![TableInfo {
                name: "column_test_table".to_string(),
                table_name: "column_test_table".to_string(),
                id: 0,
                columns: Some(vec![ColumnInfo {
                    name: column_name.to_string(),
                    data_type: None,
                }]),
            }]);

            assert_eq!(expected_result, res.is_ok());
        }
    }
}
