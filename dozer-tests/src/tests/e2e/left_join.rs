use dozer_types::grpc_types::{ingest::IngestRequest, types};
use dozer_types::grpc_types::common::QueryRequest;

use super::DozerE2eTest;

#[tokio::test]
async fn test_e2e_left_join() {
    let mut test = DozerE2eTest::new(include_str!("./fixtures/left_join.yaml")).await;

    let ingest_client = test.ingest_service_client.as_mut().unwrap();

    ingest_client
        .ingest(IngestRequest {
            schema_name: "table1".to_string(),
            new: Some(types::Record {
                values: vec![types::Value {
                    value: Some(types::value::Value::IntValue(1)),
                }],
                version: 1,
            }),
            seq_no: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    ingest_client
        .ingest(IngestRequest {
            schema_name: "table2".to_string(),
            new: Some(types::Record {
                values: vec![types::Value {
                    value: Some(types::value::Value::IntValue(1)),
                }],
                version: 1,
            }),
            seq_no: 2,
            ..Default::default()
        })
        .await
        .unwrap();

    // Wait for api to process the records.
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    let common_client = &mut test.common_service_client;

    let res = common_client
        .query(QueryRequest {
            endpoint: "table1_endpoint".to_string(),
            query: None,
        })
        .await
        .unwrap();
    let res = res.into_inner();

    assert_eq!(res.records.len(), 1);
}
