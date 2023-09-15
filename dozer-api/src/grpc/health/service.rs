use dozer_types::grpc_types::health::health_check_response::ServingStatus;
use dozer_types::grpc_types::health::health_grpc_service_server::HealthGrpcService;
use dozer_types::grpc_types::health::{HealthCheckRequest, HealthCheckResponse};
use dozer_types::tonic::{self, Request, Response, Status};
use std::collections::HashMap;
use tokio_stream::wrappers::ReceiverStream;

type ResponseStream = ReceiverStream<Result<HealthCheckResponse, Status>>;

// #[derive(Clone)]
pub struct HealthService {
    pub serving_status: HashMap<String, ServingStatus>,
}

#[tonic::async_trait]
impl HealthGrpcService for HealthService {
    async fn health_check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let req = request.into_inner();
        let service = req.service.to_lowercase();
        // currently supporting:
        // - empty string
        // - common (Common gRPC)
        // - typed (Typed gRPC)
        let serving_status = self.serving_status.get(&service);
        match serving_status {
            Some(serving_status) => {
                let status = *serving_status as i32;
                let rep = HealthCheckResponse { status };
                Ok(Response::new(rep))
            }
            None => Err(Status::not_found(service)),
        }
    }

    type healthWatchStream = ResponseStream;

    async fn health_watch(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<Self::healthWatchStream>, Status> {
        // TODO: support streaming health watch
        let req = request.into_inner();
        let _service = req.service.as_str();
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
