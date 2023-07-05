use dozer_types::grpc_types::common::QueryRequest;
use std::time::Duration;
use dozer_types::grpc_types::ingest::IngestRequest;
use dozer_types::grpc_types::types;

use super::DozerE2eTest;

#[tokio::test]
async fn test_e2e_wildcard_records() {
    let mut test = DozerE2eTest::new(include_str!("./fixtures/basic_sql_wildcard.yaml")).await;

    // Give dozer some time to process the records.
    tokio::time::sleep(Duration::from_millis(1000)).await;

    let common_client = &mut test.common_service_client;

    let res = common_client
        .query(QueryRequest {
            endpoint: "wildcard_res".to_string(),
            query: None,
        })
        .await
        .unwrap();
    let res = res.into_inner();
    assert_eq!(res.records.len(), 6);
}

#[tokio::test]
async fn test_e2e_wildcard() {
    let mut test = DozerE2eTest::new(include_str!("./fixtures/basic_sql_wildcard_err.yaml")).await;

    let ingest_client = test.ingest_service_client.as_mut().unwrap();
    ingest_client
        .ingest(IngestRequest {
            schema_name: "table3".to_string(),
            new: Some(types::Record {
                values: vec![
                    types::Value {
                        value: Some(types::value::Value::IntValue(1)),
                    },
                    types::Value {
                        value: Some(types::value::Value::IntValue(11)),
                    },
                ],
                version: 1,
            }),
            seq_no: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    ingest_client
        .ingest(IngestRequest {
            schema_name: "table4".to_string(),
            new: Some(types::Record {
                values: vec![
                    types::Value {
                        value: Some(types::value::Value::IntValue(1)),
                    },
                    types::Value {
                        value: Some(types::value::Value::IntValue(11)),
                    },
                ],
                version: 1,
            }),
            seq_no: 2,
            ..Default::default()
        })
        .await
        .unwrap();

    // Wait for api to process the records.
    tokio::time::sleep(std::time::Duration::from_millis(2000)).await;

    let common_client = &mut test.common_service_client;

    let res = common_client
        .query(QueryRequest {
            endpoint: "wildcard_res_err".to_string(),
            query: None,
        })
        .await
        .unwrap();
    let res = res.into_inner();

    assert_eq!(res.records.len(), 1);
}
