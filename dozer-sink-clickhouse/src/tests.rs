use crate::client::ClickhouseClient;
use crate::schema::ClickhouseSchema;
use clickhouse_rs::types::Query;
use dozer_core::tokio;
use dozer_types::models::sink::ClickhouseSinkConfig;
use dozer_types::types::{FieldDefinition, FieldType, Schema};

fn get_client() -> ClickhouseClient {
    ClickhouseClient::new(get_sink_config())
}

fn get_sink_config() -> ClickhouseSinkConfig {
    ClickhouseSinkConfig {
        source_table_name: "source_table".to_string(),
        sink_table_name: "sink_table".to_string(),
        scheme: "tcp".to_string(),
        create_table_options: None,
        user: "default".to_string(),
        password: None,
        database: "default".to_string(),
        host: "localhost".to_string(),
        port: 9000,
    }
}

fn _get_dozer_schema() -> Schema {
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
    let mut client = get_client().get_client_handle().await.unwrap();
    client
        .execute(&format!("DROP TABLE IF EXISTS {table_name}"))
        .await
        .unwrap();

    client
        .execute(&format!("CREATE TABLE {table_name}(id UInt64, data String, PRIMARY KEY id) ENGINE = CollapsingMergeTree ORDER BY id"))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore]
async fn test_get_clickhouse_table() {
    let client = get_client();
    let sink_config = get_sink_config();
    create_table(&sink_config.sink_table_name).await;
    let clickhouse_table = ClickhouseSchema::get_clickhouse_table(client, &sink_config)
        .await
        .unwrap();
    assert_eq!(clickhouse_table.name, sink_config.sink_table_name);
}

use clickhouse_rs::{Block, Pool};
use std::error::Error;

#[tokio::test]
#[ignore]
async fn clickhouse_test() -> Result<(), Box<dyn Error>> {
    let uuid = "248c40d9-d1eb-47c4-8801-943dbab34df9";
    let database_url = "tcp://default@localhost:9000/query_test";
    let ddl = r"
        CREATE TABLE IF NOT EXISTS payment (
            customer_id  UInt32,
            amount       UInt32,
            account_name Nullable(FixedString(3))
        ) Engine=Memory";

    let block = Block::new()
        .column("customer_id", vec![1_u32, 3, 5, 7, 9])
        .column("amount", vec![2_u32, 4, 6, 8, 10])
        .column(
            "account_name",
            vec![Some("foo"), None, None, None, Some("bar")],
        );

    let pool = Pool::new(database_url);

    let mut client = pool.get_handle().await?;
    client.execute(ddl).await?;

    let table = Query::new("payment").id(uuid);
    client.insert(table, block).await?;
    Ok(())
}
