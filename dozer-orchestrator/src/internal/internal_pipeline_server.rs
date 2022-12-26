use std::net::ToSocketAddrs;

use dozer_types::models::app_config::Config;
use tokio::runtime::Runtime;
use tonic::{transport::Server, Response};

use dozer_api::grpc::internal_grpc::{
    internal_pipeline_service_server::{self, InternalPipelineService},
    GetAppConfigRequest, GetAppConfigResponse, RestartPipelineRequest, RestartPipelineResponse,
};

pub struct InternalPipelineServer {
    app_config: Config,
}
#[tonic::async_trait]
impl InternalPipelineService for InternalPipelineServer {
    async fn get_config(
        &self,
        _request: tonic::Request<GetAppConfigRequest>,
    ) -> Result<tonic::Response<GetAppConfigResponse>, tonic::Status> {
        Ok(Response::new(GetAppConfigResponse {
            data: Some(self.app_config.to_owned()),
        }))
    }
    async fn restart(
        &self,
        _request: tonic::Request<RestartPipelineRequest>,
    ) -> Result<tonic::Response<RestartPipelineResponse>, tonic::Status> {
        todo!();
    }
}

pub fn start_internal_pipeline_server(app_config: Config) -> Result<(), tonic::transport::Error> {
    let rt = Runtime::new().unwrap();
    rt.block_on(_start_internal_pipeline_server(app_config))
}
async fn _start_internal_pipeline_server(
    app_config: Config,
) -> Result<(), tonic::transport::Error> {
    let server = InternalPipelineServer {
        app_config: app_config.to_owned(),
    };
    let internal_config = app_config
        .api
        .unwrap_or_default()
        .pipeline_internal
        .unwrap_or_default();
    let mut addr = format!("{}:{}", internal_config.host, internal_config.port)
        .to_socket_addrs()
        .unwrap();
    Server::builder()
        .add_service(internal_pipeline_service_server::InternalPipelineServiceServer::new(server))
        .serve(addr.next().unwrap())
        .await
}
