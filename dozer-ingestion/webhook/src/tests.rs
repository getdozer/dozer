use crate::connector::WebhookConnector;
use dozer_ingestion_connector::{
    dozer_types::{
        json_types::json_from_str,
        models::ingestion_types::{
            IngestionMessage, WebhookConfig, WebhookConfigSchemas, WebhookEndpoint, WebhookVerb,
        },
        serde_json::{self, json},
        types::{Field, Record},
    },
    test_util::{create_test_runtime, spawn_connector_all_tables},
    tokio::runtime::Runtime,
    IngestionIterator,
};
use std::sync::Arc;

fn ingest_webhook(
    runtime: Arc<Runtime>,
    port: u32,
) -> (
    IngestionIterator,
    dozer_ingestion_connector::futures::future::AbortHandle,
) {
    let user_schema = r#"
        {
            "users": {
              "schema": {
                "fields": [
                  {
                    "name": "id",
                    "typ": "Int",
                    "nullable": false
                  },
                  {
                    "name": "name",
                    "typ": "String",
                    "nullable": true
                  },
                  {
                    "name": "json",
                    "typ": "Json",
                    "nullable": true
                  }
                ]
              }
            }
          }
        "#;
    let customer_schema = r#"
        {
            "customers": {
              "schema": {
                "fields": [
                  {
                    "name": "id",
                    "typ": "Int",
                    "nullable": false
                  },
                  {
                    "name": "name",
                    "typ": "String",
                    "nullable": true
                  },
                  {
                    "name": "json",
                    "typ": "Json",
                    "nullable": true
                  }
                ]
              }
            }
          }
          "#;
    let webhook_connector = WebhookConnector::new(WebhookConfig {
        port: Some(port),
        host: None,
        endpoints: vec![
            WebhookEndpoint {
                path: "/customers".to_string(),
                verbs: vec![WebhookVerb::POST, WebhookVerb::DELETE],
                schema: WebhookConfigSchemas::Inline(customer_schema.to_string()),
            },
            WebhookEndpoint {
                path: "/users".to_string(),
                verbs: vec![WebhookVerb::POST, WebhookVerb::DELETE],
                schema: WebhookConfigSchemas::Inline(user_schema.to_string()),
            },
        ],
    });
    spawn_connector_all_tables(runtime.clone(), webhook_connector)
}

#[test]
fn ingest_webhook_batch_insert() {
    let runtime = create_test_runtime();
    let port = 58883;
    let result: (IngestionIterator, _) = ingest_webhook(runtime.clone(), port);
    // call http request to webhook endpoint
    let client = reqwest::blocking::Client::new();
    let post_value = json!({
        "users": [
            {
                "id": 1,
                "name": "John Doe",
                "json": {
                    "key": "value"
                }
            },
            {
                "id": 2,
                "name": "Jane Doe",
                "json": {
                    "key": "value"
                }
            }
        ]
    });
    let http_result = client
        .post(format!("http://127.0.0.1:{:}/users", port))
        .json(&post_value)
        .send();
    assert!(http_result.is_ok());
    let response = http_result.unwrap();
    assert!(response.status().is_success());
    let mut iterator = result.0;
    let msg = iterator.next().unwrap();

    let ivalue_str =
        json_from_str(&serde_json::to_string(&json!({"key": "value"})).unwrap()).unwrap();

    let expected_fields1: Vec<Field> = vec![
        Field::Int(1),
        Field::String("John Doe".to_string()),
        Field::Json(ivalue_str.clone()),
    ];
    let expected_record1 = Record {
        values: expected_fields1,
        lifetime: None,
    };

    let expected_fields2: Vec<Field> = vec![
        Field::Int(2),
        Field::String("Jane Doe".to_string()),
        Field::Json(ivalue_str),
    ];
    let expected_record2 = Record {
        values: expected_fields2,
        lifetime: None,
    };

    if let IngestionMessage::OperationEvent {
        table_index: _,
        op,
        state: _,
    } = msg
    {
        assert_eq!(
            op,
            dozer_ingestion_connector::dozer_types::types::Operation::BatchInsert {
                new: vec![expected_record1, expected_record2],
            }
        );
    } else {
        panic!("Expected operation event");
    }
}

#[test]
fn ingest_webhook_delete() {
    let runtime = create_test_runtime();
    let port = 58884;
    let result: (IngestionIterator, _) = ingest_webhook(runtime.clone(), port);
    // call http request to webhook endpoint
    let client = reqwest::blocking::Client::new();
    let delete_value = json!({
        "users": [
            {
                "id": 1,
                "name": "John Doe",
                "json": {
                    "key": "value"
                }
            }
        ]
    });
    let http_result = client
        .delete(format!("http://127.0.0.1:{:}/users", port))
        .json(&delete_value)
        .send();
    assert!(http_result.is_ok());
    let response = http_result.unwrap();
    assert!(response.status().is_success());
    let mut iterator = result.0;
    let msg = iterator.next().unwrap();
    let ivalue_str =
        json_from_str(&serde_json::to_string(&json!({"key": "value"})).unwrap()).unwrap();

    let expected_fields1: Vec<Field> = vec![
        Field::Int(1),
        Field::String("John Doe".to_string()),
        Field::Json(ivalue_str.clone()),
    ];
    let expected_record1 = Record {
        values: expected_fields1.to_owned(),
        lifetime: None,
    };

    if let IngestionMessage::OperationEvent {
        table_index: _,
        op,
        state: _,
    } = msg
    {
        if let dozer_ingestion_connector::dozer_types::types::Operation::Delete { old } = op {
            assert_eq!(old, expected_record1);
        } else {
            panic!("Expected delete operation");
        }
    } else {
        panic!("Expected operation event");
    }
}
