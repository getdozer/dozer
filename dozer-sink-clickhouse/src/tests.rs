use crate::schema::ClickhouseSchema;
use crate::ClickhouseSinkError;
use clickhouse::Client;
use dozer_core::tokio;
use dozer_types::models::sink::ClickhouseSinkConfig;
use dozer_types::types::{FieldDefinition, FieldType, Schema};

fn get_client() -> Client {
    Client::default()
        .with_url("http://localhost:8123")
        .with_user("default")
        .with_database("default")
}

fn get_sink_config() -> ClickhouseSinkConfig {
    ClickhouseSinkConfig {
        source_table_name: "source_table".to_string(),
        sink_table_name: "sink_table".to_string(),
        create_table_options: None,
        primary_keys: Some(vec!["id".to_string()]),
        user: "default".to_string(),
        password: Some("default".to_string()),
        database: "default".to_string(),
        database_url: "http://localhost:8123".to_string(),
    }
}

fn get_dozer_schema() -> Schema {
    Schema {
        fields: vec![
            FieldDefinition {
                name: "id".to_string(),
                typ: FieldType::UInt,
                nullable: false,
                source: Default::default(),
            },
            FieldDefinition {
                name: "data".to_string(),
                typ: FieldType::String,
                nullable: false,
                source: Default::default(),
            },
        ],
        primary_index: vec![0],
    }
}

async fn create_table(table_name: &str) {
    let client = get_client();
    client
        .query(&format!("DROP TABLE IF EXISTS {table_name}"))
        .execute()
        .await
        .unwrap();

    client
        .query(&format!("CREATE TABLE {table_name}(id UInt64, data String, PRIMARY KEY id) ENGINE = MergeTree ORDER BY id"))
        .execute()
        .await
        .unwrap();
}

#[tokio::test]
#[ignore]
async fn test_get_clickhouse_table() {
    let client = get_client();
    let sink_config = get_sink_config();
    create_table(&sink_config.sink_table_name).await;
    let schema = get_dozer_schema();

    let clickhouse_table = ClickhouseSchema::get_clickhouse_table(&client, &sink_config, &schema)
        .await
        .unwrap();
    assert_eq!(clickhouse_table.name, sink_config.sink_table_name);
}

#[tokio::test]
#[ignore]
async fn test_get_not_existing_clickhouse_table() {
    let client = get_client();
    let mut sink_config = get_sink_config();
    create_table("not_existing").await;
    let schema = get_dozer_schema();

    sink_config.create_table_options = None;

    let clickhouse_table =
        ClickhouseSchema::get_clickhouse_table(&client, &sink_config, &schema).await;
    eprintln!("CT {:?}", clickhouse_table);
    assert!(matches!(
        clickhouse_table,
        Err(ClickhouseSinkError::SinkTableDoesNotExist)
    ));
}
