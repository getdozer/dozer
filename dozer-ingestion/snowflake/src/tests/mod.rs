use std::time::Duration;

use dozer_ingestion_connector::{
    dozer_types::{
        models::connection::ConnectionConfig,
        types::FieldType::{Binary, Boolean, Date, Decimal, Float, Int, String, Timestamp},
    },
    test_util::{create_test_runtime, load_test_connection_config, spawn_connector},
    tokio, Connector, TableIdentifier,
};
use odbc::create_environment_v3;
use rand::Rng;

use crate::{
    connection::client::Client, connector::SnowflakeConnector, stream_consumer::StreamConsumer,
    test_utils::remove_streams,
};

const TABLE_NAME: &str = "CUSTOMERS";

#[tokio::test]
#[ignore]
async fn test_disabled_connector_and_read_from_stream() {
    let config = load_test_connection_config();
    let ConnectionConfig::Snowflake(connection) = config else {
        panic!("Snowflake config expected");
    };

    let env = create_environment_v3().map_err(|e| e.unwrap()).unwrap();
    let client = Client::new(connection.clone().into(), &env);

    let mut rng = rand::thread_rng();
    let table_name = format!("CUSTOMER_TEST_{}", rng.gen::<u32>());

    client
        .exec(&format!(
            "CREATE TABLE {table_name} LIKE SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER;"
        ))
        .unwrap();
    client.exec(&format!("ALTER TABLE PUBLIC.{table_name} ADD CONSTRAINT {table_name}_PK PRIMARY KEY (C_CUSTKEY);")).unwrap();
    client.exec(&format!("INSERT INTO {table_name} SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER LIMIT 100")).unwrap();

    remove_streams(connection.clone(), TABLE_NAME).unwrap();

    let runtime = create_test_runtime();
    let connector = SnowflakeConnector::new("snowflake".to_string(), connection.clone());
    let tables = runtime
        .block_on(
            connector.list_columns(vec![TableIdentifier::from_table_name(table_name.clone())]),
        )
        .unwrap();

    let (mut iterator, _) = spawn_connector(runtime, connector, tables);

    let mut i = 0;
    while i < 100 {
        iterator.next_timeout(Duration::from_secs(10)).await;
        i += 1;
    }

    assert_eq!(100, i);

    client.exec(&format!("INSERT INTO {table_name} SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER LIMIT 100 OFFSET 100")).unwrap();

    let mut i = 0;
    while i < 100 {
        iterator.next_timeout(Duration::from_secs(10)).await;
        i += 1;
    }

    assert_eq!(100, i);
}

#[tokio::test]
#[ignore]
async fn test_disabled_connector_get_schemas_test() {
    let config = load_test_connection_config();
    let ConnectionConfig::Snowflake(connection) = config else {
        panic!("Snowflake config expected");
    };
    let connector = SnowflakeConnector::new("snowflake".to_string(), connection.clone());
    let env = create_environment_v3().map_err(|e| e.unwrap()).unwrap();
    let client = Client::new(connection.into(), &env);

    let mut rng = rand::thread_rng();
    let table_name = format!("SCHEMA_MAPPING_TEST_{}", rng.gen::<u32>());

    client
        .exec(&format!(
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
        ))
        .unwrap();

    let table_infos = connector
        .list_columns(vec![TableIdentifier::from_table_name(table_name.clone())])
        .await
        .unwrap();
    let schemas = connector.get_schemas(&table_infos).await.unwrap();

    let source_schema = schemas[0].as_ref().unwrap();

    for field in &source_schema.schema.fields {
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

    client.exec(&format!("DROP TABLE {table_name};")).unwrap();
}

#[tokio::test]
#[ignore]
async fn test_disabled_connector_missing_table_validator() {
    let config = load_test_connection_config();
    let ConnectionConfig::Snowflake(connection) = config else {
        panic!("Snowflake config expected");
    };
    let connector = SnowflakeConnector::new("snowflake".to_string(), connection.clone());

    let not_existing_table = "not_existing_table".to_string();
    let result = connector
        .list_columns(vec![TableIdentifier::from_table_name(not_existing_table)])
        .await;

    assert!(result
        .unwrap_err()
        .to_string()
        .starts_with("table not found"));

    let table_infos = connector
        .list_columns(vec![TableIdentifier::from_table_name(
            TABLE_NAME.to_string(),
        )])
        .await
        .unwrap();
    let result = connector.get_schemas(&table_infos).await.unwrap();

    assert!(result[0].is_ok());
}

#[tokio::test]
#[ignore]
async fn test_disabled_connector_is_stream_created() {
    let config = load_test_connection_config();
    let ConnectionConfig::Snowflake(connection) = config else {
        panic!("Snowflake config expected");
    };

    let env = create_environment_v3().map_err(|e| e.unwrap()).unwrap();
    let client = Client::new(connection.into(), &env);

    let mut rng = rand::thread_rng();
    let table_name = format!("STREAM_EXIST_TEST_{}", rng.gen::<u32>());

    client
        .exec(&format!(
            "CREATE TABLE {table_name} (id  INTEGER)
                        data_retention_time_in_days = 0; "
        ))
        .unwrap();

    let result = StreamConsumer::is_stream_created(&client, &table_name).unwrap();
    assert!(
        !result,
        "Stream was not created yet, so result of check should be false"
    );

    StreamConsumer::create_stream(&client, &table_name).unwrap();
    let result = StreamConsumer::is_stream_created(&client, &table_name).unwrap();
    assert!(
        result,
        "Stream is created, so result of check should be true"
    );

    StreamConsumer::drop_stream(&client, &table_name).unwrap();
    let result = StreamConsumer::is_stream_created(&client, &table_name).unwrap();
    assert!(
        !result,
        "Stream was dropped, so result of check should be false"
    );
}
