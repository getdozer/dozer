use dozer_ingestion_connector::tokio;
use dozer_ingestion_connector::utils::ListOrFilterColumns;
use rand::Rng;
use serial_test::serial;
use std::collections::HashSet;

use crate::schema::helper::SchemaHelper;
use crate::test_utils::load_test_connection_config;
use crate::tests::client::TestPostgresClient;
use crate::{PostgresConnectorError, PostgresSchemaError};

macro_rules! assert_vec_eq {
    ($a:expr, $b:expr) => {{
        let a: HashSet<_> = $a.iter().cloned().collect();
        let b: HashSet<_> = $b.iter().cloned().collect();

        assert_eq!(a, b)
    }};
}

#[tokio::test]
#[ignore]
#[serial]
async fn test_connector_get_tables() {
    let config = load_test_connection_config().await;
    let mut client = TestPostgresClient::new(&config).await;

    let mut rng = rand::thread_rng();

    let schema = format!("schema_helper_test_{}", rng.gen::<u32>());
    let table_name = format!("products_test_{}", rng.gen::<u32>());

    client.create_schema(&schema).await;
    client.create_simple_table(&schema, &table_name).await;

    let schema_helper = SchemaHelper::new(client.postgres_config.clone(), None);
    let result = schema_helper.get_tables(None).await.unwrap();

    let table = result.first().unwrap();
    assert_eq!(table_name, table.name);
    assert_vec_eq!(
        &[
            "name".to_string(),
            "description".to_string(),
            "weight_single".to_string(),
            "weight_double".to_string(),
            "id".to_string(),
            "index2".to_string(),
            "index4".to_string(),
            "index8".to_string(),
        ],
        &table.columns
    );

    client.drop_schema(&schema).await;
}

#[tokio::test]
#[ignore]
#[serial]
async fn test_connector_get_schema_with_selected_columns() {
    let config = load_test_connection_config().await;
    let mut client = TestPostgresClient::new(&config).await;

    let mut rng = rand::thread_rng();

    let schema = format!("schema_helper_test_{}", rng.gen::<u32>());
    let table_name = format!("products_test_{}", rng.gen::<u32>());

    client.create_schema(&schema).await;
    client.create_simple_table(&schema, &table_name).await;

    let schema_helper = SchemaHelper::new(client.postgres_config.clone(), None);
    let table_info = ListOrFilterColumns {
        schema: Some(schema.clone()),
        name: table_name.clone(),
        columns: Some(vec!["name".to_string(), "id".to_string()]),
    };
    let result = schema_helper.get_tables(Some(&[table_info])).await.unwrap();

    let table = result.first().unwrap();
    assert_eq!(table_name, table.name);
    assert_vec_eq!(&["name".to_string(), "id".to_string()], &table.columns);

    client.drop_schema(&schema).await;
}

#[tokio::test]
#[ignore]
#[serial]
async fn test_connector_get_schema_without_selected_columns() {
    let config = load_test_connection_config().await;
    let mut client = TestPostgresClient::new(&config).await;

    let mut rng = rand::thread_rng();

    let schema = format!("schema_helper_test_{}", rng.gen::<u32>());
    let table_name = format!("products_test_{}", rng.gen::<u32>());

    client.create_schema(&schema).await;
    client.create_simple_table(&schema, &table_name).await;

    let schema_helper = SchemaHelper::new(client.postgres_config.clone(), None);
    let table_info = ListOrFilterColumns {
        name: table_name.clone(),
        schema: Some(schema.clone()),
        columns: Some(vec![]),
    };
    let result = schema_helper.get_tables(Some(&[table_info])).await.unwrap();

    let table = result.first().unwrap();
    assert_eq!(table_name, table.name.clone());
    assert_vec_eq!(
        &[
            "id".to_string(),
            "name".to_string(),
            "description".to_string(),
            "weight_single".to_string(),
            "weight_double".to_string(),
            "index2".to_string(),
            "index4".to_string(),
            "index8".to_string(),
        ],
        &table.columns
    );

    client.drop_schema(&schema).await;
}

#[tokio::test]
#[ignore]
#[serial]
async fn test_connector_view_cannot_be_used() {
    let config = load_test_connection_config().await;
    let mut client = TestPostgresClient::new(&config).await;

    let mut rng = rand::thread_rng();

    let schema = format!("schema_helper_test_{}", rng.gen::<u32>());
    let table_name = format!("products_test_{}", rng.gen::<u32>());
    let view_name = format!("products_view_test_{}", rng.gen::<u32>());

    client.create_schema(&schema).await;
    client.create_simple_table(&schema, &table_name).await;
    client.create_view(&schema, &table_name, &view_name).await;

    let schema_helper = SchemaHelper::new(client.postgres_config.clone(), None);
    let table_info = ListOrFilterColumns {
        name: view_name,
        schema: Some(schema.clone()),
        columns: Some(vec![]),
    };

    let result = schema_helper.get_schemas(&[table_info]).await;
    assert!(
        result.is_err(),
        "Result is not an error. Result: {:?}",
        result
    );
    assert!(matches!(
        result.unwrap().first().unwrap(),
        Err(PostgresConnectorError::PostgresSchemaError(
            PostgresSchemaError::UnsupportedTableType(_, _)
        ))
    ));

    let table_info = ListOrFilterColumns {
        name: table_name,
        schema: Some(schema.clone()),
        columns: Some(vec![]),
    };
    let result = schema_helper.get_schemas(&[table_info]).await;
    assert!(result.unwrap().first().unwrap().is_ok());

    client.drop_schema(&schema).await;
}
