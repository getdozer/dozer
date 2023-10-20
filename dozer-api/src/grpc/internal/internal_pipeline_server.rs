use async_stream::stream;
use dozer_cache::dozer_log::home_dir::BuildId;
use dozer_cache::dozer_log::replication::{Log, LogResponseFuture};
use dozer_types::bincode;
use dozer_types::grpc_types::internal::internal_pipeline_service_server::{
    InternalPipelineService, InternalPipelineServiceServer,
};
use dozer_types::grpc_types::internal::{
    BuildRequest, BuildResponse, DescribeApplicationRequest, DescribeApplicationResponse,
    EndpointResponse, EndpointsResponse, GetIdResponse, LogRequest, LogResponse, StorageRequest,
    StorageResponse,
};
use dozer_types::log::info;
use dozer_types::models::api_config::{
    default_app_grpc_host, default_app_grpc_port, AppGrpcOptions,
};
use dozer_types::models::api_endpoint::ApiEndpoint;
use dozer_types::tonic::transport::server::TcpIncoming;
use dozer_types::tonic::transport::Server;
use dozer_types::tonic::{self, Request, Response, Status, Streaming};
use futures_util::stream::BoxStream;
use futures_util::{Future, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use crate::errors::GrpcError;
use crate::grpc::run_server;
use crate::shutdown::ShutdownReceiver;

#[derive(Debug, Clone)]
pub struct LogEndpoint {
    pub build_id: BuildId,
    pub schema_string: String,
    pub descriptor_bytes: Vec<u8>,
    pub log: Arc<Mutex<Log>>,
}

#[derive(Debug)]
pub struct InternalPipelineServer {
    id: String,
    endpoints: HashMap<String, LogEndpoint>,
}

impl InternalPipelineServer {
    pub fn new(endpoints: HashMap<String, LogEndpoint>) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            endpoints,
        }
    }
}

#[tonic::async_trait]
impl InternalPipelineService for InternalPipelineServer {
    async fn get_id(&self, _: Request<()>) -> Result<Response<GetIdResponse>, Status> {
        Ok(Response::new(GetIdResponse {
            id: self.id.clone(),
        }))
    }

    async fn describe_storage(
        &self,
        request: Request<StorageRequest>,
    ) -> Result<Response<StorageResponse>, Status> {
        let endpoint = request.into_inner().endpoint;
        let log = &find_log_endpoint(&self.endpoints, &endpoint)?.log;
        let storage = log.lock().await.describe_storage();
        Ok(Response::new(StorageResponse {
            storage: Some(storage),
        }))
    }

    async fn list_endpoints(
        &self,
        _request: Request<()>,
    ) -> Result<Response<EndpointsResponse>, Status> {
        let endpoints = self
            .endpoints
            .iter()
            .map(|(endpoint, log)| EndpointResponse {
                endpoint: endpoint.clone(),
                build_name: log.build_id.name().to_string(),
            })
            .collect();
        Ok(Response::new(EndpointsResponse { endpoints }))
    }
    async fn describe_application(
        &self,
        _: Request<DescribeApplicationRequest>,
    ) -> Result<Response<DescribeApplicationResponse>, Status> {
        let mut endpoints = HashMap::with_capacity(self.endpoints.len());

        for (endpoint, log_endpoint) in &self.endpoints {
            let build_response = get_build_response(log_endpoint);
            endpoints.insert(endpoint.clone(), build_response);
        }

        Ok(Response::new(DescribeApplicationResponse { endpoints }))
    }

    async fn describe_build(
        &self,
        request: Request<BuildRequest>,
    ) -> Result<Response<BuildResponse>, Status> {
        let endpoint = request.into_inner().endpoint;
        let endpoint = find_log_endpoint(&self.endpoints, &endpoint)?;
        let build_response = get_build_response(endpoint);
        Ok(Response::new(build_response))
    }

    type GetLogStream = BoxStream<'static, Result<LogResponse, Status>>;

    async fn get_log(
        &self,
        requests: Request<Streaming<LogRequest>>,
    ) -> Result<Response<Self::GetLogStream>, Status> {
        let endpoints = self.endpoints.clone();
        let requests = requests.into_inner();
        let stream = stream! {
            for await request in requests {
                let request = match request {
                    Ok(request) => request,
                    Err(e) => {
                        yield Err(e);
                        continue;
                    }
                };

                let log = &match find_log_endpoint(&endpoints, &request.endpoint) {
                    Ok(log) => log,
                    Err(e) => {
                        yield Err(e);
                        continue;
                    }
                }.log;

                let response = log.lock().await.read(
                    request.start as usize..request.end as usize,
                    Duration::from_millis(request.timeout_in_millis as u64),
                    log.clone(),
                ).await;
                yield serialize_log_response(response).await;
            }
        };
        Ok(Response::new(stream.boxed()))
    }
}

fn get_build_response(endpoint: &LogEndpoint) -> BuildResponse {
    BuildResponse {
        name: endpoint.build_id.name().to_owned(),
        schema_string: endpoint.schema_string.clone(),
        descriptor_bytes: endpoint.descriptor_bytes.clone(),
    }
}

fn find_log_endpoint<'a>(
    endpoints: &'a HashMap<String, LogEndpoint>,
    endpoint: &str,
) -> Result<&'a LogEndpoint, Status> {
    endpoints.get(endpoint).ok_or_else(|| {
        Status::new(
            tonic::Code::NotFound,
            format!("Endpoint {} not found", endpoint),
        )
    })
}

async fn serialize_log_response(response: LogResponseFuture) -> Result<LogResponse, Status> {
    let response = response
        .await
        .map_err(|e| Status::new(tonic::Code::Internal, e.to_string()))?;
    let data = bincode::serialize(&response).map_err(|e| {
        Status::new(
            tonic::Code::Internal,
            format!("Failed to serialize response: {}", e),
        )
    })?;
    Ok(LogResponse { data })
}

/// TcpIncoming::new requires a tokio runtime, so we mark this function as async.
pub async fn start_internal_pipeline_server(
    endpoint_and_logs: Vec<(ApiEndpoint, LogEndpoint)>,
    options: &AppGrpcOptions,
    shutdown: ShutdownReceiver,
) -> Result<impl Future<Output = Result<(), tonic::transport::Error>>, GrpcError> {
    let endpoints = endpoint_and_logs
        .into_iter()
        .map(|(endpoint, log)| (endpoint.name, log))
        .collect();
    let server = InternalPipelineServer::new(endpoints);

    // Start listening.
    let addr = format!(
        "{}:{}",
        options.host.clone().unwrap_or_else(default_app_grpc_host),
        options.port.unwrap_or_else(default_app_grpc_port)
    );
    info!("Starting Internal Server on {addr}");
    let addr = addr
        .parse()
        .map_err(|e| GrpcError::AddrParse(addr.clone(), e))?;
    let incoming = TcpIncoming::new(addr, true, None).map_err(|e| GrpcError::Listen(addr, e))?;

    // Run server.
    let server = Server::builder().add_service(InternalPipelineServiceServer::new(server));
    Ok(run_server(server, incoming, shutdown))
}
