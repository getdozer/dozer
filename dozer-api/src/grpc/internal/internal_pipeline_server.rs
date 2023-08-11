use dozer_cache::dozer_log::home_dir::BuildId;
use dozer_cache::dozer_log::replication::Log;
use dozer_types::bincode;
use dozer_types::grpc_types::internal::internal_pipeline_service_server::{
    InternalPipelineService, InternalPipelineServiceServer,
};
use dozer_types::grpc_types::internal::{
    BuildRequest, BuildResponse, EndpointResponse, EndpointsResponse, LogRequest, LogResponse,
    StorageRequest, StorageResponse,
};
use dozer_types::log::info;
use dozer_types::models::api_config::AppGrpcOptions;
use dozer_types::models::api_endpoint::ApiEndpoint;
use futures_util::future::Either;
use futures_util::stream::{AbortHandle, Abortable, Aborted, BoxStream};
use futures_util::{Future, StreamExt, TryStreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use crate::errors::GrpcError;

#[derive(Debug, Clone)]
pub struct LogEndpoint {
    pub build_id: BuildId,
    pub schema_string: String,
    pub descriptor_bytes: Vec<u8>,
    pub log: Arc<Mutex<Log>>,
}

#[derive(Debug)]
pub struct InternalPipelineServer {
    endpoints: HashMap<String, LogEndpoint>,
}

impl InternalPipelineServer {
    pub fn new(endpoints: HashMap<String, LogEndpoint>) -> Self {
        Self { endpoints }
    }
}

#[tonic::async_trait]
impl InternalPipelineService for InternalPipelineServer {
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

    async fn describe_build(
        &self,
        request: Request<BuildRequest>,
    ) -> Result<Response<BuildResponse>, Status> {
        let endpoint = request.into_inner().endpoint;
        let endpoint = find_log_endpoint(&self.endpoints, &endpoint)?;
        Ok(Response::new(BuildResponse {
            name: endpoint.build_id.name().to_string(),
            schema_string: endpoint.schema_string.clone(),
            descriptor_bytes: endpoint.descriptor_bytes.clone(),
        }))
    }

    type GetLogStream = BoxStream<'static, Result<LogResponse, Status>>;

    async fn get_log(
        &self,
        requests: Request<Streaming<LogRequest>>,
    ) -> Result<Response<Self::GetLogStream>, Status> {
        let endpoints = self.endpoints.clone();
        Ok(Response::new(
            requests
                .into_inner()
                .and_then(move |request| {
                    let log = &match find_log_endpoint(&endpoints, &request.endpoint) {
                        Ok(log) => log,
                        Err(e) => return Either::Left(std::future::ready(Err(e))),
                    }
                    .log;
                    Either::Right(get_log(log.clone(), request))
                })
                .boxed(),
        ))
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

async fn get_log(log: Arc<Mutex<Log>>, request: LogRequest) -> Result<LogResponse, Status> {
    let mut log_mut = log.lock().await;
    let response = log_mut.read(
        request.start as usize..request.end as usize,
        Duration::from_millis(request.timeout_in_millis as u64),
        log.clone(),
    );
    // Must drop log before awaiting response, otherwise we will deadlock.
    drop(log_mut);
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

pub async fn start_internal_pipeline_server(
    endpoint_and_logs: Vec<(ApiEndpoint, LogEndpoint)>,
    options: &AppGrpcOptions,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), GrpcError> {
    let endpoints = endpoint_and_logs
        .into_iter()
        .map(|(endpoint, log)| (endpoint.name, log))
        .collect();
    let server = InternalPipelineServer::new(endpoints);

    // Tonic graceful shutdown doesn't allow us to set a timeout, resulting in hanging if a client doesn't close the connection.
    // So we just abort the server when the shutdown signal is received.
    let (abort_handle, abort_registration) = AbortHandle::new_pair();
    tokio::spawn(async move {
        shutdown.await;
        abort_handle.abort();
    });

    // Run server.
    let addr = format!("{}:{}", options.host, options.port);
    info!("Starting Internal Server on {addr}");
    let addr = addr
        .parse()
        .map_err(|e| GrpcError::AddrParse(addr.clone(), e))?;
    let server = Server::builder().add_service(InternalPipelineServiceServer::new(server));
    match Abortable::new(server.serve(addr), abort_registration).await {
        Ok(result) => result.map_err(GrpcError::Transport),
        Err(Aborted) => Ok(()),
    }
}
