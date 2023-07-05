use dozer_types::grpc_types::common::QueryRequest;
use std::time::Duration;

use super::DozerE2eTest;

#[tokio::test]
async fn test_e2e_wildcard() {
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
