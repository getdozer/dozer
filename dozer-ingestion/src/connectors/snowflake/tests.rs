use crate::connectors::snowflake::test_utils::{get_client, remove_streams};
use crate::connectors::{get_connector, TableInfo};
use crate::ingestion::{IngestionConfig, Ingestor};

use dozer_types::types::FieldType::{
    Binary, Boolean, Date, Decimal, Float, Int, String, Timestamp,
};

use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::models::connection::ConnectionConfig;
use odbc::create_environment_v3;
use rand::Rng;
use std::thread;

use crate::errors::ConnectorError::TableNotFound;
use crate::test_util::run_connector_test;

use crate::connectors::snowflake::connection::client::Client;
use crate::connectors::snowflake::stream_consumer::StreamConsumer;

#[ignore]
#[test]
fn test_connector_and_read_from_stream() {
    run_connector_test("snowflake", |config| {
        let connection = config.connections.get(0).unwrap();
        let source = config.sources.get(0).unwrap().clone();

        let client = get_client(connection);

        let env = create_environment_v3().map_err(|e| e.unwrap()).unwrap();
        let conn = env
            .connect_with_connection_string(&client.get_conn_string())
            .unwrap();

        let mut rng = rand::thread_rng();
        let table_name = format!("CUSTOMER_TEST_{}", rng.gen::<u32>());

        client
            .execute_query(
                &conn,
                &format!(
                    "CREATE TABLE {table_name} LIKE SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER;"
                ),
            )
            .unwrap();
        client.execute_query(&conn, &format!("ALTER TABLE PUBLIC.{table_name} ADD CONSTRAINT {table_name}_PK PRIMARY KEY (C_CUSTKEY);")).unwrap();
        client.execute_query(&conn, &format!("INSERT INTO {table_name} SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER LIMIT 100")).unwrap();

        remove_streams(connection.clone(), &source.table_name).unwrap();

        let config = IngestionConfig::default();

        let (ingestor, mut iterator) = Ingestor::initialize_channel(config);

        let connection_config = connection.clone();
        let table = TableInfo {
            name: table_name.clone(),
            table_name: table_name.clone(),
            id: 0,
            columns: None,
        };
        thread::spawn(move || {
            let tables: Vec<TableInfo> = vec![table];

            let connector = get_connector(connection_config).unwrap();
            let _ = connector.start(None, &ingestor, tables);
        });

        let mut i = 0;
        while i < 100 {
            let op = iterator.next();
            match op {
                None => {}
                Some(IngestionMessage { identifier, .. }) => {
                    assert_eq!(identifier.txid, 0);
                    assert_eq!(identifier.seq_in_tx, i);
                }
            }
            i += 1;
        }

        assert_eq!(100, i);

        client.execute_query(&conn, &format!("INSERT INTO {table_name} SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER LIMIT 100 OFFSET 100")).unwrap();

        let mut i = 0;
        while i < 100 {
            let op = iterator.next();
            match op {
                None => {}
                Some(IngestionMessage { identifier, .. }) => {
                    assert_eq!(identifier.txid, 1);
                    assert_eq!(identifier.seq_in_tx, i);
                }
            }
            i += 1;
        }

        assert_eq!(100, i);
    });
}

#[ignore]
#[test]
fn test_connector_get_schemas_test() {
    run_connector_test("snowflake", |config| {
        let connection = config.connections.get(0).unwrap();
        let connector = get_connector(connection.clone()).unwrap();
        let client = get_client(connection);

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

        let source_schema = schemas.get(0).unwrap();
        assert_eq!(source_schema.name, table_name);

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

        client
            .execute_query(&conn, &format!("DROP TABLE {table_name};"))
            .unwrap();
    });
}

#[ignore]
#[test]
fn test_connector_missing_table_validator() {
    run_connector_test("snowflake", |config| {
        let connection = config.connections.get(0).unwrap();
        let connector = get_connector(connection.clone()).unwrap();

        let not_existing_table = "not_existing_table".to_string();
        let result = connector
            .validate_schemas(&[TableInfo {
                name: not_existing_table.clone(),
                table_name: not_existing_table,
                id: 0,
                columns: None,
            }])
            .unwrap();

        let error = result.get("not_existing_table").unwrap().get(0).unwrap();
        assert_eq!(error.0, None);
        assert!(error.1.is_err());
        assert!(matches!(error.1, Err(TableNotFound(_))));

        let existing_table = &config.sources.get(0).unwrap().table_name;
        let result = connector
            .validate_schemas(&[TableInfo {
                name: existing_table.clone(),
                table_name: existing_table.clone(),
                id: 0,
                columns: None,
            }])
            .unwrap();

        let errors = result.get(existing_table).unwrap();
        assert!(errors.is_empty());
    });
}

#[ignore]
#[test]
fn test_connector_is_stream_created() {
    run_connector_test("snowflake", |config| {
        let connection = config.connections.get(0).unwrap();
        let snowflake_config = match connection.config.as_ref().unwrap() {
            ConnectionConfig::Snowflake(snowflake_config) => snowflake_config.clone(),
            _ => {
                panic!("Snowflake config expected");
            }
        };

        let client = Client::new(&snowflake_config);
        let env = create_environment_v3().map_err(|e| e.unwrap()).unwrap();
        let conn = env
            .connect_with_connection_string(&client.get_conn_string())
            .unwrap();

        let mut rng = rand::thread_rng();
        let table_name = format!("STREAM_EXIST_TEST_{}", rng.gen::<u32>());

        client
            .execute_query(
                &conn,
                &format!(
                    "CREATE TABLE {table_name} (id  INTEGER)
                        data_retention_time_in_days = 0; "
                ),
            )
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
    });
}
