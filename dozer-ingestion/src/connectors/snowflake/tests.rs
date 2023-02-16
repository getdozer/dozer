use crate::connectors::snowflake::stream_consumer::StreamConsumer;
use crate::connectors::snowflake::test_utils::{get_client, remove_streams};
use crate::connectors::{get_connector, TableInfo};
use crate::ingestion::{IngestionConfig, Ingestor};
use dozer_types::models::app_config::Config;

use dozer_types::serde_yaml;
use dozer_types::types::FieldType::{
    Binary, Boolean, Date, Decimal, Float, Int, String, Timestamp,
};
use dozer_types::types::Schema;
use odbc::create_environment_v3;
use rand::Rng;
use std::thread;

use crate::test_util::load_config;

#[ignore]
#[test]
// fn connector_e2e_connect_snowflake_and_read_from_stream() {
fn connector_disabled_test_e2e_connect_snowflake_and_read_from_stream() {
    let config = serde_yaml::from_str::<Config>(load_config("test.snowflake.yaml")).unwrap();
    let connection = config.connections.get(0).unwrap().clone();
    let source = config.sources.get(0).unwrap().clone();
    remove_streams(connection.clone(), &source.table_name).unwrap();

    let config = IngestionConfig::default();

    let (ingestor, mut iterator) = Ingestor::initialize_channel(config);

    thread::spawn(move || {
        let tables: Vec<TableInfo> = vec![TableInfo {
            name: source.table_name.clone(),
            table_name: source.table_name,
            id: 0,
            columns: None,
        }];

        let connector = get_connector(connection).unwrap();
        let _ = connector.start(None, &ingestor, Some(tables));
    });

    let mut i = 0;
    while i < 1000 {
        i += 1;
        let op = iterator.next();
        match op {
            None => {}
            Some((_, _operation)) => {}
        }
    }

    assert_eq!(1000, i);
}

#[ignore]
#[test]
// fn connector_e2e_connect_snowflake_schema_changes_test() {
fn connector_disabled_test_e2e_connect_snowflake_schema_changes_test() {
    let config = serde_yaml::from_str::<Config>(load_config("test.snowflake.yaml")).unwrap();
    let connection = config.connections.get(0).unwrap().clone();
    let client = get_client(&connection);

    let (ingestor, mut iterator) = Ingestor::initialize_channel(IngestionConfig::default());

    let mut rng = rand::thread_rng();
    let table_name = format!("schema_change_test_{}", rng.gen::<u32>());
    let stream_name = StreamConsumer::get_stream_table_name(&table_name);

    let env = create_environment_v3().map_err(|e| e.unwrap()).unwrap();
    let conn = env
        .connect_with_connection_string(&client.get_conn_string())
        .unwrap();

    client
        .execute_query(
            &conn,
            &format!("CREATE TABLE {table_name} LIKE SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION;"),
        )
        .unwrap();
    client
        .execute_query(
            &conn,
            &format!("CREATE STREAM {stream_name} ON TABLE {table_name}"),
        )
        .unwrap();

    // Create new stream
    let mut consumer = StreamConsumer::new();
    consumer
        .consume_stream(&client, &table_name, &ingestor, 0, 1)
        .unwrap();

    // Insert single record
    client.execute_query(&conn, &format!("INSERT INTO {table_name} (N_NATIONKEY, N_COMMENT, N_REGIONKEY, N_NAME) VALUES (1, 'TEST Country 1', 0, 'country name 1');")).unwrap();
    consumer
        .consume_stream(&client, &table_name, &ingestor, 0, 1)
        .unwrap();
    iterator.next().unwrap();

    // Update table and insert record
    client
        .execute_query(
            &conn,
            &format!("ALTER TABLE {table_name} ADD TEST_COLUMN INTEGER;"),
        )
        .unwrap();
    client.execute_query(&conn, &format!("INSERT INTO {table_name} (N_NATIONKEY, N_COMMENT, N_REGIONKEY, N_NAME, TEST_COLUMN) VALUES (2, 'TEST Country 2', 0, 'country name 2', null);")).unwrap();

    consumer
        .consume_stream(&client, &table_name, &ingestor, 0, 1)
        .unwrap();
    iterator.next().unwrap();

    client
        .execute_query(&conn, &format!("DROP TABLE {table_name};"))
        .unwrap();
}

#[ignore]
#[test]
// fn connector_e2e_connect_snowflake_get_schemas_test() {
fn connector_disabled_test_e2e_connect_snowflake_get_schemas_test() {
    let config = serde_yaml::from_str::<Config>(load_config("test.snowflake.yaml")).unwrap();
    let connection = config.connections.get(0).unwrap().clone();
    let client = get_client(&connection);
    let connector = get_connector(connection).unwrap();

    let env = create_environment_v3().map_err(|e| e.unwrap()).unwrap();
    let conn = env
        .connect_with_connection_string(&client.get_conn_string())
        .unwrap();

    let mut rng = rand::thread_rng();
    let table_name = format!("SCHEMA_MAPPING_TEST_{}", rng.gen::<u32>());

    client
        .execute_query(
            &conn,
            &format!(
                "create table {table_name}
        (
            integer_column  integer,
            float_column    float,
            text_column     varchar,
            binary_column   binary,
            boolean_column  boolean,
            date_column     date,
            datetime_column datetime,
            decimal_column  decimal(5, 2)
        )
            data_retention_time_in_days = 0;

        "
            ),
        )
        .unwrap();

    let schemas = connector
        .as_ref()
        .get_schemas(Some(vec![TableInfo {
            name: table_name.to_string(),
            table_name: table_name.to_string(),
            id: 0,
            columns: None,
        }]))
        .unwrap();

    let (schema_name, Schema { fields, .. }, _) = schemas.get(0).unwrap();
    assert_eq!(schema_name, &table_name);
    for field in fields {
        let expected_type = match field.name.as_str() {
            "INTEGER_COLUMN" => Int,
            "FLOAT_COLUMN" => Float,
            "TEXT_COLUMN" => String,
            "BINARY_COLUMN" => Binary,
            "BOOLEAN_COLUMN" => Boolean,
            "DATE_COLUMN" => Date,
            "DATETIME_COLUMN" => Timestamp,
            "DECIMAL_COLUMN" => Decimal,
            _ => {
                panic!("Unexpected column: {}", field.name)
            }
        };

        assert_eq!(expected_type, field.typ);
    }

    client
        .execute_query(&conn, &format!("DROP TABLE {table_name};"))
        .unwrap();
}
