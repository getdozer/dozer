use dozer_cache::dozer_log::replication::Log;
use dozer_cache::dozer_log::storage::Storage;
use dozer_types::bincode;
use dozer_types::grpc_types::internal::internal_pipeline_service_server::{
    self, InternalPipelineService,
};
use dozer_types::grpc_types::internal::{LogRequest, LogResponse};
use dozer_types::log::info;
use dozer_types::models::api_config::AppGrpcOptions;
use dozer_types::models::api_endpoint::ApiEndpoint;
use futures_util::future::Either;
use futures_util::stream::{AbortHandle, Abortable, Aborted, BoxStream};
use futures_util::{Future, StreamExt, TryStreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use crate::errors::GrpcError;

pub struct InternalPipelineServer<S: Storage> {
    endpoints: HashMap<String, Arc<Mutex<Log<S>>>>,
}

impl<S: Storage> InternalPipelineServer<S> {
    pub fn new(endpoints: HashMap<String, Arc<Mutex<Log<S>>>>) -> Self {
        Self { endpoints }
    }
}

#[tonic::async_trait]
impl<S: Storage> InternalPipelineService for InternalPipelineServer<S> {
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
                    let log = match endpoints.get(&request.endpoint) {
                        Some(log) => log,
                        None => {
                            return Either::Left(std::future::ready(Err(Status::new(
                                tonic::Code::NotFound,
                                format!("Endpoint {} not found", request.endpoint),
                            ))))
                        }
                    };
                    Either::Right(get_log(log.clone(), request))
                })
                .boxed(),
        ))
    }
}

async fn get_log<S: Storage>(
    log: Arc<Mutex<Log<S>>>,
    request: LogRequest,
) -> Result<LogResponse, Status> {
    let mut log = log.lock().await;
    let response = log.read(request.start as usize..request.end as usize);
    // Must drop log before awaiting response, otherwise we will deadlock.
    drop(log);
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

pub async fn start_internal_pipeline_server<S: Storage>(
    endpoint_and_logs: &[(ApiEndpoint, Arc<Mutex<Log<S>>>)],
    options: &AppGrpcOptions,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), GrpcError> {
    let endpoints = endpoint_and_logs
        .iter()
        .map(|(endpoint, log)| (endpoint.name.clone(), log.clone()))
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
    let server = Server::builder()
        .add_service(internal_pipeline_service_server::InternalPipelineServiceServer::new(server));
    match Abortable::new(server.serve(addr), abort_registration).await {
        Ok(result) => result.map_err(GrpcError::Transport),
        Err(Aborted) => Ok(()),
    }
}
