use crate::{connector::ReplicationSlotInfo, PostgresConnectorError};

use super::{client::Client, tables_validator::TablesValidator};
use dozer_ingestion_connector::{
    dozer_types::indicatif::{ProgressBar, ProgressStyle},
    utils::ListOrFilterColumns,
};
use postgres_types::PgLsn;
use regex::Regex;

pub enum Validations {
    Details,
    User,
    Tables,
    WALLevel,
    Slot,
}

pub async fn validate_connection(
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
    let pb = ProgressBar::new(validations_order.len() as u64);
    pb.set_style(
        ProgressStyle::with_template(&format!(
            "[{}] {}",
            name, "{spinner:.green} {wide_msg} {bar}"
        ))
        .unwrap(),
    );
    pb.set_message("Validating connection to source");

    let mut client = super::helper::connect(config).await?;

    for validation_type in validations_order {
        match validation_type {
            Validations::Details => validate_details(&mut client).await?,
            Validations::User => validate_user(&mut client).await?,
            Validations::Tables => {
                if let Some(tables_info) = &tables {
                    validate_tables(&mut client, tables_info).await?;
                }
            }
            Validations::WALLevel => validate_wal_level(&mut client).await?,
            Validations::Slot => {
                if let Some(replication_details) = &replication_info {
                    validate_slot(&mut client, replication_details, tables).await?;
                } else {
                    validate_limit_of_replications(&mut client).await?;
                }
            }
        }

        pb.inc(1);
    }

    pb.finish_and_clear();

    Ok(())
}

async fn validate_details(client: &mut Client) -> Result<(), PostgresConnectorError> {
    client
        .simple_query("SELECT version()")
        .await
        .map_err(PostgresConnectorError::ConnectionFailure)?;

    Ok(())
}

async fn validate_user(client: &mut Client) -> Result<(), PostgresConnectorError> {
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
        .await
        .map_or(Err(PostgresConnectorError::ReplicationIsNotAvailableForUserError), |row| {
            let can_login: bool = row.get("can_login");
            let is_replication_role: bool = row.get("is_replication_role");
            let is_aws_replication_role: bool = row.get("is_aws_replication_role");

            if can_login && (is_replication_role || is_aws_replication_role) {
                Ok(())
            } else {
                Err(PostgresConnectorError::ReplicationIsNotAvailableForUserError)
            }
        })
}

async fn validate_wal_level(client: &mut Client) -> Result<(), PostgresConnectorError> {
    let result = client
        .query_one("SHOW wal_level", &[])
        .await
        .map_err(|_e| PostgresConnectorError::WALLevelIsNotCorrect())?;

    let wal_level: Result<String, _> = result.try_get(0);
    wal_level.map_or_else(
        |e| Err(PostgresConnectorError::InvalidQueryError(e)),
        |level| {
            if level == "logical" {
                Ok(())
            } else {
                Err(PostgresConnectorError::WALLevelIsNotCorrect())
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
            return Err(PostgresConnectorError::TableNameNotValid(t.name.clone()));
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
                    return Err(PostgresConnectorError::ColumnNameNotValid(column.clone()));
                }
            }
        }
    }

    Ok(())
}

async fn validate_tables(
    client: &mut Client,
    table_info: &Vec<ListOrFilterColumns>,
) -> Result<(), PostgresConnectorError> {
    validate_tables_names(table_info)?;
    validate_columns_names(table_info)?;

    let tables_validator = TablesValidator::new(table_info);
    tables_validator.validate(client).await?;

    Ok(())
}

pub async fn validate_slot(
    client: &mut Client,
    replication_info: &ReplicationSlotInfo,
    tables: Option<&Vec<ListOrFilterColumns>>,
) -> Result<(), PostgresConnectorError> {
    let result = client
        .query_one(
            "SELECT active, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
            &[&replication_info.name],
        )
        .await
        .map_err(PostgresConnectorError::InvalidQueryError)?;

    let is_already_running: bool = result
        .try_get(0)
        .map_err(PostgresConnectorError::InvalidQueryError)?;
    if is_already_running {
        return Err(PostgresConnectorError::SlotIsInUseError(
            replication_info.name.clone(),
        ));
    }

    let flush_lsn: PgLsn = result
        .try_get(1)
        .map_err(PostgresConnectorError::InvalidQueryError)?;

    if flush_lsn.gt(&replication_info.start_lsn) {
        return Err(PostgresConnectorError::StartLsnIsBeforeLastFlushedLsnError(
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
            .await
            .map_err(|_e| {
                PostgresConnectorError::SlotNotExistError(replication_info.name.clone())
            })?;

        let mut publication_tables: Vec<String> = vec![];
        for row in result {
            publication_tables.push(row.get(0));
        }

        for t in tables_list {
            if !publication_tables.contains(&t.name) {
                return Err(PostgresConnectorError::MissingTableInReplicationSlot(
                    t.name.clone(),
                ));
            }
        }
    }

    Ok(())
}

async fn validate_limit_of_replications(client: &mut Client) -> Result<(), PostgresConnectorError> {
    let slots_limit_result = client
        .query_one("SHOW max_replication_slots", &[])
        .await
        .map_err(PostgresConnectorError::ConnectionFailure)?;

    let slots_limit_str: String = slots_limit_result
        .try_get(0)
        .map_err(PostgresConnectorError::InvalidQueryError)?;
    let slots_limit: i64 = slots_limit_str.parse().unwrap();

    let used_slots_result = client
        .query_one("SELECT COUNT(*) FROM pg_replication_slots;", &[])
        .await
        .map_err(PostgresConnectorError::ConnectionFailure)?;

    let used_slots: i64 = used_slots_result
        .try_get(0)
        .map_err(PostgresConnectorError::InvalidQueryError)?;

    if used_slots == slots_limit {
        Err(PostgresConnectorError::NoAvailableSlotsError)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        connection::helper::{connect, map_connection_config},
        test_utils::load_test_connection_config,
        tests::client::TestPostgresClient,
        PostgresSchemaError,
    };

    use super::*;

    use dozer_ingestion_connector::tokio;
    use postgres_types::PgLsn;
    use rand::Rng;
    use serial_test::serial;
    use std::panic;

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_validation_connection_fail_to_connect() {
        let config = load_test_connection_config().await;
        let mut config = map_connection_config(&config).unwrap();
        config.dbname("not_existing");

        let result = validate_connection("pg_test_conn", config, None, None).await;

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

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_validation_connection_requested_tables_not_exist() {
        let config = load_test_connection_config().await;
        let config = map_connection_config(&config).unwrap();
        let mut client = connect(config.clone()).await.unwrap();

        client
            .simple_query("DROP TABLE IF EXISTS not_existing")
            .await
            .expect("User creation failed");

        let tables = vec![ListOrFilterColumns {
            name: "not_existing".to_string(),
            schema: Some("public".to_string()),
            columns: None,
        }];
        let result = validate_connection("pg_test_conn", config, Some(&tables), None).await;

        assert!(result.is_err());

        match result {
            Ok(_) => panic!("Validation should fail"),
            Err(e) => {
                assert!(matches!(e, PostgresConnectorError::TablesNotFound(_)));

                if let PostgresConnectorError::TablesNotFound(msg) = e {
                    assert_eq!(
                        msg,
                        vec![("public".to_string(), "not_existing".to_string())]
                    );
                } else {
                    panic!("Unexpected error occurred");
                }
            }
        }
    }

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_validation_connection_requested_columns_not_exist() {
        let config = load_test_connection_config().await;
        let config = map_connection_config(&config).unwrap();
        let mut client = connect(config.clone()).await.unwrap();

        client
                .simple_query("CREATE TABLE IF NOT EXISTS existing(column_1 serial PRIMARY KEY, column_2 serial);")
                .await
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

        let result = validate_connection("pg_test_conn", config, Some(&tables), None).await;

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
    }

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_validation_connection_replication_slot_not_exist() {
        let config = load_test_connection_config().await;
        let config = map_connection_config(&config).unwrap();

        let new_slot = "not_existing_slot";
        let replication_info = ReplicationSlotInfo {
            name: new_slot.to_string(),
            start_lsn: PgLsn::from(0),
        };

        let result =
            validate_connection("pg_test_conn", config, None, Some(replication_info)).await;

        assert!(result.is_err());

        match result {
            Ok(_) => panic!("Validation should fail"),
            Err(e) => {
                assert!(matches!(e, PostgresConnectorError::InvalidQueryError(_)));
            }
        }
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

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_validation_connection_valid_number_of_replication_slots() {
        let config = load_test_connection_config().await;
        let config = map_connection_config(&config).unwrap();
        let mut client = connect(config.clone()).await.unwrap();

        let slots_limit_result = client
            .query_one("SHOW max_replication_slots", &[])
            .await
            .unwrap();

        let slots_limit_str: String = slots_limit_result.try_get(0).unwrap();
        let slots_limit: i64 = slots_limit_str.parse().unwrap();

        let used_slots_result = client
            .query_one("SELECT COUNT(*) FROM pg_replication_slots;", &[])
            .await
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
                .await
                .unwrap();
        }

        // One replication slot is available
        let result = validate_connection("pg_test_conn", config, None, None).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_validation_connection_not_any_replication_slot_availble() {
        let config = load_test_connection_config().await;
        let config = map_connection_config(&config).unwrap();
        let mut client = connect(config.clone()).await.unwrap();

        let slots_limit_result = client
            .query_one("SHOW max_replication_slots", &[])
            .await
            .unwrap();

        let slots_limit_str: String = slots_limit_result.try_get(0).unwrap();
        let slots_limit: i64 = slots_limit_str.parse().unwrap();

        let used_slots_result = client
            .query_one("SELECT COUNT(*) FROM pg_replication_slots;", &[])
            .await
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
                .await
                .unwrap();
        }

        let result = validate_connection("pg_test_conn", config, None, None).await;

        assert!(result.is_err());

        match result {
            Ok(_) => panic!("Validation should fail"),
            Err(e) => {
                assert!(matches!(e, PostgresConnectorError::NoAvailableSlotsError));
            }
        }
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

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_return_error_on_view_in_table_validation() {
        let config = load_test_connection_config().await;
        let mut client = TestPostgresClient::new(&config).await;

        let mut rng = rand::thread_rng();

        let schema = format!("schema_helper_test_{}", rng.gen::<u32>());
        let table_name = format!("products_test_{}", rng.gen::<u32>());
        let view_name = format!("products_view_test_{}", rng.gen::<u32>());

        client.create_schema(&schema).await;
        client.create_simple_table(&schema, &table_name).await;
        client.create_view(&schema, &table_name, &view_name).await;

        let config = map_connection_config(&config).unwrap();
        let mut pg_client = connect(config).await.unwrap();

        let result = validate_tables(
            &mut pg_client,
            &vec![ListOrFilterColumns {
                name: table_name,
                schema: Some(schema.clone()),
                columns: None,
            }],
        )
        .await;

        assert!(result.is_ok());

        let result = validate_tables(
            &mut pg_client,
            &vec![ListOrFilterColumns {
                name: view_name,
                schema: Some(schema),
                columns: None,
            }],
        )
        .await;

        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(PostgresConnectorError::PostgresSchemaError(
                PostgresSchemaError::UnsupportedTableType(_, _)
            ))
        ));
    }
}
