use dozer_services::{common::QueryRequest, ingest::IngestRequest, types};

use super::DozerE2eTest;

#[tokio::test]
async fn ingest_and_test() {
    let mut test = DozerE2eTest::new(include_str!("./fixtures/basic.yaml")).await;

    let ingest_client = test.ingest_service_client.as_mut().unwrap();

    // Ingest a record
    let res = ingest_client
        .ingest(IngestRequest {
            schema_name: "users".to_string(),
            new: vec![
                types::Value {
                    value: Some(types::value::Value::IntValue(1675)),
                },
                types::Value {
                    value: Some(types::value::Value::StringValue("dario".to_string())),
                },
            ],
            seq_no: 1,
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(res.into_inner().seq_no, 1);

    // wait for the record to be processed
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    // Query common service
    let common_client = &mut test.common_service_client;

    let res = common_client
        .query(QueryRequest {
            endpoint: "users".to_string(),
            query: None,
        })
        .await
        .unwrap();
    let res = res.into_inner();
    let rec = res.records.first().unwrap().clone();
    let val = rec.values.first().unwrap().clone().value;
    assert!(matches!(val, Some(types::value::Value::IntValue(_))));

    if let Some(types::value::Value::IntValue(v)) = val {
        assert_eq!(v, 1675);
    }
}
