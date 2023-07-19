use dozer_cache::dozer_log::home_dir::BuildPath;
use dozer_cache::dozer_log::replication::Log;
use dozer_types::bincode;
use dozer_types::grpc_types::internal::internal_pipeline_service_server::{
    InternalPipelineService, InternalPipelineServiceServer,
};
use dozer_types::grpc_types::internal::{
    BuildRequest, BuildResponse, LogRequest, LogResponse, StorageRequest, StorageResponse,
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
pub struct BuildAndLog {
    pub build: BuildPath,
    pub log: Arc<Mutex<Log>>,
}

#[derive(Debug)]
pub struct InternalPipelineServer {
    endpoints: HashMap<String, BuildAndLog>,
}

impl InternalPipelineServer {
    pub fn new(endpoints: HashMap<String, BuildAndLog>) -> Self {
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
        let log = &find_build_and_log(&self.endpoints, &endpoint)?.log;
        let storage = log.lock().await.describe_storage();
        Ok(Response::new(StorageResponse {
            storage: Some(storage),
        }))
    }

    async fn describe_build(
        &self,
        request: Request<BuildRequest>,
    ) -> Result<Response<BuildResponse>, Status> {
        let endpoint = request.into_inner().endpoint;
        let build = &find_build_and_log(&self.endpoints, &endpoint)?.build;
        let name = build.id.name().to_string();
        let schema_string = tokio::fs::read_to_string(&build.schema_path)
            .await
            .map_err(|e| {
                Status::new(
                    tonic::Code::Internal,
                    format!("Failed to read schema: {}", e),
                )
            })?;
        let descriptor_bytes = tokio::fs::read(&build.descriptor_path).await.map_err(|e| {
            Status::new(
                tonic::Code::Internal,
                format!("Failed to read descriptor: {}", e),
            )
        })?;
        Ok(Response::new(BuildResponse {
            name,
            schema_string,
            descriptor_bytes,
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
                    let log = &match find_build_and_log(&endpoints, &request.endpoint) {
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

fn find_build_and_log<'a>(
    endpoints: &'a HashMap<String, BuildAndLog>,
    endpoint: &str,
) -> Result<&'a BuildAndLog, Status> {
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
    endpoint_and_logs: Vec<(ApiEndpoint, BuildAndLog)>,
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
