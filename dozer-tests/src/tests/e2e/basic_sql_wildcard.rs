use dozer_types::grpc_types::{ingest::IngestRequest, types};

use super::DozerE2eTest;

#[tokio::test]
#[ignore = "Wildcard implementation may have a bug. This test fails about one out of 5 times"]
async fn test_e2e_wildcard() {
    let mut test = DozerE2eTest::new(include_str!("./fixtures/basic_sql_wildcard.yaml")).await;

    let ingest_client = test.ingest_service_client.as_mut().unwrap();

    ingest_client
        .ingest(IngestRequest {
            schema_name: "table3".to_string(),
            new: vec![
                types::Value {
                    value: Some(types::value::Value::IntValue(1)),
                },
                types::Value {
                    value: Some(types::value::Value::IntValue(11)),
                },
            ],
            seq_no: 1,
            ..Default::default()
        })
        .await
        .unwrap();

    ingest_client
        .ingest(IngestRequest {
            schema_name: "table4".to_string(),
            new: vec![
                types::Value {
                    value: Some(types::value::Value::IntValue(1)),
                },
                types::Value {
                    value: Some(types::value::Value::IntValue(11)),
                },
            ],
            seq_no: 2,
            ..Default::default()
        })
        .await
        .unwrap();

    // Wait for api to process the records.
    tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
}
