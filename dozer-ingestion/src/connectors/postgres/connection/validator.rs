use crate::connectors::postgres::connector::ReplicationSlotInfo;
use crate::connectors::ListOrFilterColumns;

use crate::errors::PostgresConnectorError::{
    ColumnNameNotValid, ConnectionFailure, InvalidQueryError, MissingTableInReplicationSlot,
    NoAvailableSlotsError, ReplicationIsNotAvailableForUserError, SlotIsInUseError,
    SlotNotExistError, StartLsnIsBeforeLastFlushedLsnError, TableNameNotValid,
    WALLevelIsNotCorrect,
};

use crate::connectors::postgres::connection::tables_validator::TablesValidator;
use crate::errors::PostgresConnectorError;
use dozer_types::indicatif::ProgressStyle;
use postgres::Client;
use postgres_types::PgLsn;
use regex::Regex;
use std::borrow::BorrowMut;

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
    tables: Option<&Vec<ListOrFilterColumns>>,
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

fn validate_tables_names(
    table_info: &Vec<ListOrFilterColumns>,
) -> Result<(), PostgresConnectorError> {
    let table_regex = Regex::new(r"^([[:lower:]_][[:alnum:]_]*)$").unwrap();
    for t in table_info {
        if !table_regex.is_match(&t.name) {
            return Err(TableNameNotValid(t.name.clone()));
        }
    }

    Ok(())
}

fn validate_columns_names(
    table_info: &Vec<ListOrFilterColumns>,
) -> Result<(), PostgresConnectorError> {
    let column_name_regex = Regex::new(r"^([[:lower:]_][[:alnum:]_]*)$").unwrap();
    for t in table_info {
        if let Some(columns) = &t.columns {
            for column in columns {
                if !column_name_regex.is_match(column) {
                    return Err(ColumnNameNotValid(column.clone()));
                }
            }
        }
    }

    Ok(())
}

fn validate_tables(
    client: &mut Client,
    table_info: &Vec<ListOrFilterColumns>,
) -> Result<(), PostgresConnectorError> {
    validate_tables_names(table_info)?;
    validate_columns_names(table_info)?;

    let tables_validator = TablesValidator::new(table_info);
    tables_validator.validate(client)?;

    Ok(())
}

pub fn validate_slot(
    client: &mut Client,
    replication_info: &ReplicationSlotInfo,
    tables: Option<&Vec<ListOrFilterColumns>>,
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
            if !publication_tables.contains(&t.name) {
                return Err(MissingTableInReplicationSlot(t.name.clone()));
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
    use super::*;

    use crate::connectors::postgres::connector::ReplicationSlotInfo;
    use crate::connectors::postgres::test_utils::get_client;
    use crate::errors::PostgresConnectorError;
    use crate::errors::PostgresSchemaError::UnsupportedTableType;
    use crate::test_util::{get_config, run_connector_test};
    use postgres_types::PgLsn;
    use rand::Rng;
    use serial_test::serial;
    use std::panic;
    use tokio_postgres::NoTls;

    #[test]
    #[ignore]
    #[serial]
    fn test_connector_validation_connection_fail_to_connect() {
        run_connector_test("postgres", |app_config| {
            let mut config = get_config(app_config);
            config.dbname("not_existing");

            let result = validate_connection("pg_test_conn", config, None, None);

            assert!(result.is_err());

            match result {
                Ok(_) => panic!("Validation should fail"),
                Err(e) => {
                    assert!(matches!(e, PostgresConnectorError::ConnectionFailure(_)));

                    if let PostgresConnectorError::ConnectionFailure(msg) = e {
                        assert_eq!(
                            msg.to_string(),
                            "db error: FATAL: database \"not_existing\" does not exist"
                        );
                    } else {
                        panic!("Unexpected error occurred");
                    }
                }
            }
        });
    }

    // #[test]
    // #[ignore]
    // #[serial]
    // fn test_connector_validation_connection_user_not_have_permission_to_use_replication() {
    //     run_connector_test("postgres", |app_config| {
    //         let mut config = get_config(app_config);
    //         let mut client = postgres::Config::from(config.clone())
    //             .connect(NoTls)
    //             .unwrap();
    //
    //         client
    //             .simple_query("DROP USER if exists dozer_test_without_permission")
    //             .expect("User delete failed");
    //
    //         client
    //             .simple_query("CREATE USER dozer_test_without_permission")
    //             .expect("User creation failed");
    //
    //         client
    //             .simple_query("ALTER ROLE dozer_test_without_permission WITH NOREPLICATION")
    //             .expect("Role update failed");
    //
    //         config.user("dozer_test_without_permission");
    //
    //         let result = validate_connection(config, None, None);
    //
    //         assert!(result.is_err());
    //
    //         match result {
    //             Ok(_) => panic!("Validation should fail"),
    //             Err(e) => {
    //                 assert!(matches!(
    //                     e,
    //                     PostgresConnectorError::ReplicationIsNotAvailableForUserError
    //                 ));
    //             }
    //         }
    //     });
    // }

    #[test]
    #[ignore]
    #[serial]
    fn test_connector_validation_connection_requested_tables_not_exist() {
        run_connector_test("postgres", |app_config| {
            let config = get_config(app_config);
            let mut client = postgres::Config::from(config.clone())
                .connect(NoTls)
                .unwrap();

            client
                .simple_query("DROP TABLE IF EXISTS not_existing")
                .expect("User creation failed");

            let tables = vec![ListOrFilterColumns {
                name: "not_existing".to_string(),
                schema: Some("public".to_string()),
                columns: None,
            }];
            let result = validate_connection("pg_test_conn", config, Some(&tables), None);

            assert!(result.is_err());

            match result {
                Ok(_) => panic!("Validation should fail"),
                Err(e) => {
                    assert!(matches!(e, PostgresConnectorError::TableError(_)));

                    if let PostgresConnectorError::TableError(msg) = e {
                        assert_eq!(msg, vec!["public.not_existing".to_string()]);
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
    fn test_connector_validation_connection_requested_columns_not_exist() {
        run_connector_test("postgres", |app_config| {
            let config = get_config(app_config);
            let mut client = postgres::Config::from(config.clone())
                .connect(NoTls)
                .unwrap();

            client
                .simple_query("CREATE TABLE IF NOT EXISTS existing(column_1 serial PRIMARY KEY, column_2 serial);")
                .expect("User creation failed");

            let columns = vec![
                String::from("column_not_existing_1"),
                String::from("column_not_existing_2"),
            ];

            let tables = vec![ListOrFilterColumns {
                name: "existing".to_string(),
                schema: Some("public".to_string()),
                columns: Some(columns),
            }];

            let result = validate_connection("pg_test_conn", config, Some(&tables), None);

            assert!(result.is_err());

            match result {
                Ok(_) => panic!("Validation should fail"),
                Err(e) => {
                    assert!(matches!(e, PostgresConnectorError::ColumnsNotFound(_)));

                    if let PostgresConnectorError::ColumnsNotFound(msg) = e {
                        assert_eq!(msg, "column_not_existing_1 in public.existing table, column_not_existing_2 in public.existing table");
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
    fn test_connector_validation_connection_replication_slot_not_exist() {
        run_connector_test("postgres", |app_config| {
            let config = get_config(app_config);

            let new_slot = "not_existing_slot";
            let replication_info = ReplicationSlotInfo {
                name: new_slot.to_string(),
                start_lsn: PgLsn::from(0),
            };

            let result = validate_connection("pg_test_conn", config, None, Some(replication_info));

            assert!(result.is_err());

            match result {
                Ok(_) => panic!("Validation should fail"),
                Err(e) => {
                    assert!(matches!(e, PostgresConnectorError::InvalidQueryError(_)));
                }
            }
        });
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
        // let result = validate_connection(config, None, Some(replication_info));
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
    fn test_connector_validation_connection_valid_number_of_replication_slots() {
        run_connector_test("postgres", |app_config| {
            let config = get_config(app_config);
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
                let slot_name = format!("slot_{n}");
                client
                    .query(
                        r#"SELECT pg_create_logical_replication_slot($1, 'pgoutput');"#,
                        &[&slot_name],
                    )
                    .unwrap();
            }

            // One replication slot is available
            let result = validate_connection("pg_test_conn", config, None, None);
            assert!(result.is_ok());
        });
    }

    #[test]
    #[ignore]
    #[serial]
    fn test_connector_validation_connection_not_any_replication_slot_availble() {
        run_connector_test("postgres", |app_config| {
            let config = get_config(app_config);
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

            let range = used_slots..slots_limit;
            for n in range {
                let slot_name = format!("slot_{n}");
                client
                    .query(
                        r#"SELECT pg_create_logical_replication_slot($1, 'pgoutput');"#,
                        &[&slot_name],
                    )
                    .unwrap();
            }

            let result = validate_connection("pg_test_conn", config, None, None);

            assert!(result.is_err());

            match result {
                Ok(_) => panic!("Validation should fail"),
                Err(e) => {
                    assert!(matches!(e, PostgresConnectorError::NoAvailableSlotsError));
                }
            }
        });
    }

    #[test]
    fn test_connector_validate_tables_names_with_valid_tables_names() {
        let tables_with_result = vec![
            ("test", true),
            ("Test", false),
            (";Drop table test", false),
            ("test_with_underscore", true),
        ];

        for (table_name, expected_result) in tables_with_result {
            let res = validate_tables_names(&vec![ListOrFilterColumns {
                name: table_name.to_string(),
                schema: Some("public".to_string()),
                columns: None,
            }]);

            assert_eq!(expected_result, res.is_ok());
        }
    }

    #[test]
    fn test_connector_validate_columns_names_with_valid_column_names() {
        let columns_names_with_result = vec![
            ("test", true),
            ("Test", false),
            (";Drop table test", false),
            ("test_with_underscore", true),
        ];

        for (column_name, expected_result) in columns_names_with_result {
            let res = validate_columns_names(&vec![ListOrFilterColumns {
                schema: Some("public".to_string()),
                name: "column_test_table".to_string(),
                columns: Some(vec![column_name.to_string()]),
            }]);

            assert_eq!(expected_result, res.is_ok());
        }
    }

    #[test]
    #[ignore]
    #[serial]
    fn test_connector_return_error_on_view_in_table_validation() {
        run_connector_test("postgres", |app_config| {
            let mut client = get_client(app_config.clone());

            let mut rng = rand::thread_rng();

            let schema = format!("schema_helper_test_{}", rng.gen::<u32>());
            let table_name = format!("products_test_{}", rng.gen::<u32>());
            let view_name = format!("products_view_test_{}", rng.gen::<u32>());

            client.create_schema(&schema);
            client.create_simple_table(&schema, &table_name);
            client.create_view(&schema, &table_name, &view_name);

            let config = get_config(app_config);
            let mut pg_client = postgres::Config::from(config).connect(NoTls).unwrap();

            let result = validate_tables(
                &mut pg_client,
                &vec![ListOrFilterColumns {
                    name: table_name,
                    schema: Some(schema.clone()),
                    columns: None,
                }],
            );

            assert!(result.is_ok());

            let result = validate_tables(
                &mut pg_client,
                &vec![ListOrFilterColumns {
                    name: view_name,
                    schema: Some(schema),
                    columns: None,
                }],
            );

            assert!(result.is_err());
            assert!(matches!(
                result,
                Err(PostgresConnectorError::PostgresSchemaError(
                    UnsupportedTableType(_, _)
                ))
            ));
        });
    }
}
