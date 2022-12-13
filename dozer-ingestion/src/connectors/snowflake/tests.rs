use crate::connectors::snowflake::stream_consumer::StreamConsumer;
use crate::connectors::snowflake::test_utils::{get_client, remove_streams};
use crate::connectors::{get_connector, TableInfo};
use crate::ingestion::{IngestionConfig, Ingestor};
use dozer_types::ingestion_types::IngestionOperation;
use dozer_types::models::source::Source;
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
fn connector_e2e_connect_snowflake_and_read_from_stream() {
    let source = serde_yaml::from_str::<Source>(load_config("test.snowflake.yaml")).unwrap();
    remove_streams(source.connection.clone(), &source.table_name).unwrap();

    let config = IngestionConfig::default();

    let (ingestor, iterator) = Ingestor::initialize_channel(config);

    thread::spawn(|| {
        let tables: Vec<TableInfo> = vec![TableInfo {
            name: source.table_name,
            id: 0,
            columns: None,
        }];

        let mut connector = get_connector(source.connection).unwrap();
        connector.initialize(ingestor, Some(tables)).unwrap();
        connector.start().unwrap();
    });

    let mut i = 0;
    while i < 1000 {
        i += 1;
        let op = iterator.write().next();
        match op {
            None => {}
            Some((_, ingestion_operation)) => {
                match ingestion_operation {
                    IngestionOperation::OperationEvent(_) => {
                        // Assuming that only first message is schema update
                        assert_ne!(i, 1);
                    }
                    IngestionOperation::SchemaUpdate(_, _) => {
                        assert_eq!(i, 1)
                    }
                }
            }
        }
    }

    assert_eq!(1000, i);
}

#[ignore]
#[test]
fn connector_e2e_connect_snowflake_schema_changes_test() {
    let source = serde_yaml::from_str::<Source>(load_config("test.snowflake.yaml")).unwrap();
    let client = get_client(&source.connection);

    let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());

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
            &format!(
                "CREATE TABLE {} LIKE SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION;",
                table_name
            ),
        )
        .unwrap();
    client
        .execute_query(
            &conn,
            &format!("CREATE STREAM {} ON TABLE {}", stream_name, table_name),
        )
        .unwrap();

    // Create new stream
    let mut consumer = StreamConsumer::new(0);
    consumer
        .consume_stream(&client, &table_name, &ingestor)
        .unwrap();
    assert!(matches!(
        iterator.write().next().unwrap().1,
        IngestionOperation::SchemaUpdate(_, _)
    ));

    // Insert single record
    client.execute_query(&conn, &format!("INSERT INTO {} (N_NATIONKEY, N_COMMENT, N_REGIONKEY, N_NAME) VALUES (1, 'TEST Country 1', 0, 'country name 1');", table_name)).unwrap();
    consumer
        .consume_stream(&client, &table_name, &ingestor)
        .unwrap();
    assert!(matches!(
        iterator.write().next().unwrap().1,
        IngestionOperation::OperationEvent(_)
    ));

    // Update table and insert record
    client
        .execute_query(
            &conn,
            &format!("ALTER TABLE {} ADD TEST_COLUMN INTEGER;", table_name),
        )
        .unwrap();
    client.execute_query(&conn, &format!("INSERT INTO {} (N_NATIONKEY, N_COMMENT, N_REGIONKEY, N_NAME, TEST_COLUMN) VALUES (2, 'TEST Country 2', 0, 'country name 2', null);", table_name)).unwrap();

    consumer
        .consume_stream(&client, &table_name, &ingestor)
        .unwrap();
    assert!(matches!(
        iterator.write().next().unwrap().1,
        IngestionOperation::SchemaUpdate(_, _)
    ));
    assert!(matches!(
        iterator.write().next().unwrap().1,
        IngestionOperation::OperationEvent(_)
    ));

    client
        .execute_query(&conn, &format!("DROP TABLE {};", table_name))
        .unwrap();
}

#[ignore]
#[test]
fn connector_e2e_connect_snowflake_get_schemas_test() {
    let source = serde_yaml::from_str::<Source>(load_config("test.snowflake.yaml")).unwrap();
    let client = get_client(&source.connection);
    let connector = get_connector(source.connection).unwrap();

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
                "create table {}
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

        ",
                table_name
            ),
        )
        .unwrap();

    let schemas = connector
        .as_ref()
        .get_schemas(Some(vec![TableInfo {
            name: table_name.to_string(),
            id: 0,
            columns: None,
        }]))
        .unwrap();

    let (schema_name, Schema { fields, .. }) = schemas.get(0).unwrap();
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
        .execute_query(&conn, &format!("DROP TABLE {};", table_name))
        .unwrap();
}
