use std::time::Duration;

use crate::connectors::snowflake::connector::SnowflakeConnector;
use crate::connectors::snowflake::test_utils::remove_streams;
use crate::connectors::{get_connector, Connector, TableIdentifier};

use dozer_types::types::FieldType::{
    Binary, Boolean, Date, Decimal, Float, Int, String, Timestamp,
};

use dozer_types::models::connection::ConnectionConfig;
use odbc::create_environment_v3;
use rand::Rng;

use crate::errors::ConnectorError::TableNotFound;
use crate::test_util::{create_test_runtime, run_connector_test, spawn_connector};

use crate::connectors::snowflake::connection::client::Client;
use crate::connectors::snowflake::stream_consumer::StreamConsumer;

#[tokio::test]
#[ignore]
async fn test_disabled_connector_and_read_from_stream() {
    run_connector_test("snowflake", |config| async move {
        let connection = config.connections.get(0).unwrap();
        let ConnectionConfig::Snowflake(connection) = connection.config.as_ref().unwrap() else {
            panic!("Snowflake config expected");
        };
        let source = config.sources.get(0).unwrap().clone();

        let env = create_environment_v3().map_err(|e| e.unwrap()).unwrap();
        let client = Client::new(connection, &env);

        let mut rng = rand::thread_rng();
        let table_name = format!("CUSTOMER_TEST_{}", rng.gen::<u32>());

        client
            .exec(
                &format!(
                    "CREATE TABLE {table_name} LIKE SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER;"
                ),
            )
            .unwrap();
        client.exec(&format!("ALTER TABLE PUBLIC.{table_name} ADD CONSTRAINT {table_name}_PK PRIMARY KEY (C_CUSTKEY);")).unwrap();
        client.exec(&format!("INSERT INTO {table_name} SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER LIMIT 100")).unwrap();

        remove_streams(connection, &source.table_name).unwrap();

        let runtime = create_test_runtime();
        let connector = SnowflakeConnector::new("snowflake".to_string(), connection.clone());
        let tables = runtime.block_on(connector.list_columns(vec![TableIdentifier::from_table_name(table_name.clone())])).unwrap();

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
    }).await
}

#[tokio::test]
#[ignore]
async fn test_disabled_connector_get_schemas_test() {
    run_connector_test("snowflake", |config| async move {
        let connection = config.connections.get(0).unwrap();
        let ConnectionConfig::Snowflake(connection) = connection.config.as_ref().unwrap() else {
            panic!("Snowflake config expected");
        };
        let connector = SnowflakeConnector::new("snowflake".to_string(), connection.clone());
        let env = create_environment_v3().map_err(|e| e.unwrap()).unwrap();
        let client = Client::new(connection, &env);

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

        let source_schema = schemas.get(0).unwrap().as_ref().unwrap();

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
    })
    .await
}

#[tokio::test]
#[ignore]
async fn test_disabled_connector_missing_table_validator() {
    run_connector_test("snowflake", |config| async move {
        let connection = config.connections.get(0).unwrap();
        let connector = get_connector(connection.clone()).unwrap();

        let not_existing_table = "not_existing_table".to_string();
        let result = connector
            .list_columns(vec![TableIdentifier::from_table_name(not_existing_table)])
            .await;

        assert!(matches!(result.unwrap_err(), TableNotFound(_)));

        let existing_table = &config.sources.get(0).unwrap().table_name;
        let table_infos = connector
            .list_columns(vec![TableIdentifier::from_table_name(
                existing_table.clone(),
            )])
            .await
            .unwrap();
        let result = connector.get_schemas(&table_infos).await.unwrap();

        assert!(result.get(0).unwrap().is_ok());
    })
    .await
}

#[tokio::test]
#[ignore]
async fn test_disabled_connector_is_stream_created() {
    run_connector_test("snowflake", |config| async move {
        let connection = config.connections.get(0).unwrap();
        let snowflake_config = match connection.config.as_ref().unwrap() {
            ConnectionConfig::Snowflake(snowflake_config) => snowflake_config.clone(),
            _ => {
                panic!("Snowflake config expected");
            }
        };

        let env = create_environment_v3().map_err(|e| e.unwrap()).unwrap();
        let client = Client::new(&snowflake_config, &env);

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
    })
    .await
}
