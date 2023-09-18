use dozer_types::tonic::Request;
use std::collections::HashMap;

use dozer_types::grpc_types::health::health_check_response::ServingStatus;
use dozer_types::grpc_types::health::health_grpc_service_server::HealthGrpcService;
use dozer_types::grpc_types::health::HealthCheckRequest;

use super::HealthService;

fn setup_health_service() -> HealthService {
    let mut serving_status = HashMap::new();
    serving_status.insert("".to_string(), ServingStatus::Serving);
    HealthService { serving_status }
}

#[tokio::test]
async fn test_grpc_health_check() {
    let service = setup_health_service();
    let response = service
        .health_check(Request::new(HealthCheckRequest {
            service: "".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(response.status.eq(&(ServingStatus::Serving as i32)));
    let response = service
        .health_check(Request::new(HealthCheckRequest {
            service: "non-existent".to_string(),
        }))
        .await;
    assert_eq!(
        response.unwrap_err().code(),
        dozer_types::tonic::Code::NotFound
    );
}
