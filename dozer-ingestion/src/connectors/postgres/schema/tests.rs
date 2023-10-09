use crate::connectors::postgres::schema::helper::SchemaHelper;
use crate::connectors::postgres::test_utils::get_client;
use crate::connectors::ListOrFilterColumns;
use crate::errors::PostgresConnectorError::PostgresSchemaError;
use crate::errors::PostgresSchemaError::UnsupportedTableType;
use crate::test_util::run_connector_test;
use rand::Rng;
use serial_test::serial;
use std::collections::HashSet;
use std::hash::Hash;

fn assert_vec_eq<T>(a: &[T], b: &[T]) -> bool
where
    T: Eq + Hash,
{
    let a: HashSet<_> = a.iter().collect();
    let b: HashSet<_> = b.iter().collect();

    a == b
}

#[tokio::test]
#[ignore]
#[serial]
async fn test_connector_get_tables() {
    run_connector_test("postgres", |app_config| async move {
        let mut client = get_client(app_config).await;

        let mut rng = rand::thread_rng();

        let schema = format!("schema_helper_test_{}", rng.gen::<u32>());
        let table_name = format!("products_test_{}", rng.gen::<u32>());

        client.create_schema(&schema).await;
        client.create_simple_table(&schema, &table_name).await;

        let schema_helper = SchemaHelper::new(client.postgres_config.clone(), None);
        let result = schema_helper.get_tables(None).await.unwrap();

        let table = result.get(0).unwrap();
        assert_eq!(table_name, table.name);
        assert!(assert_vec_eq(
            &[
                "name".to_string(),
                "description".to_string(),
                "weight".to_string(),
                "id".to_string(),
            ],
            &table.columns
        ));

        client.drop_schema(&schema).await;
    })
    .await
}

#[tokio::test]
#[ignore]
#[serial]
async fn test_connector_get_schema_with_selected_columns() {
    run_connector_test("postgres", |app_config| async move {
        let mut client = get_client(app_config).await;

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

        let table = result.get(0).unwrap();
        assert_eq!(table_name, table.name);
        assert!(assert_vec_eq(
            &["name".to_string(), "id".to_string()],
            &table.columns
        ));

        client.drop_schema(&schema).await;
    })
    .await
}

#[tokio::test]
#[ignore]
#[serial]
async fn test_connector_get_schema_without_selected_columns() {
    run_connector_test("postgres", |app_config| async move {
        let mut client = get_client(app_config).await;

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

        let table = result.get(0).unwrap();
        assert_eq!(table_name, table.name.clone());
        assert!(assert_vec_eq(
            &[
                "id".to_string(),
                "name".to_string(),
                "description".to_string(),
                "weight".to_string(),
            ],
            &table.columns
        ));

        client.drop_schema(&schema).await;
    })
    .await
}

#[tokio::test]
#[ignore]
#[serial]
async fn test_connector_view_cannot_be_used() {
    run_connector_test("postgres", |app_config| async move {
        let mut client = get_client(app_config).await;

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
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(PostgresSchemaError(UnsupportedTableType(_, _)))
        ));

        let table_info = ListOrFilterColumns {
            name: table_name,
            schema: Some(schema.clone()),
            columns: Some(vec![]),
        };
        let result = schema_helper.get_schemas(&[table_info]).await;
        assert!(result.is_ok());

        client.drop_schema(&schema).await;
    })
    .await
}
