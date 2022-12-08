use super::internal_grpc::{
    self, internal_pipeline_service_client::InternalPipelineServiceClient,
    internal_pipeline_service_server::InternalPipelineService, GetConfigRequest, GetConfigResponse,
    PipelineRequest, PipelineResponse,
};
use crossbeam::channel::Receiver;
use dozer_types::{
    crossbeam::channel::Sender,
    log::{self, debug},
    models::{api_config::ApiInternal, app_config::Config},
    serde_yaml,
};
use log::warn;
use std::{net::ToSocketAddrs, thread, time::Duration};
use tokio::runtime::Runtime;
use tokio_stream::StreamExt;
use tonic::{transport::Server, Response, Status};
pub struct InternalServer {
    sender: Sender<PipelineRequest>,
    app_config: Config,
}

type PipelineResult<T> = Result<Response<T>, Status>;

#[tonic::async_trait]
impl InternalPipelineService for InternalServer {
    async fn stream_pipeline_request(
        &self,
        request: tonic::Request<tonic::Streaming<PipelineRequest>>,
    ) -> PipelineResult<PipelineResponse> {
        let mut in_stream = request.into_inner();

        // let (sender, receiver) = channel::unbounded::<PipelineRequest>();

        while let Some(result) = in_stream.next().await {
            match result {
                Ok(msg) => {
                    if self.sender.send(msg).is_err() {
                        warn!("on_internal_grpc_stream send error");
                        // receiver drop
                        break;
                    }
                }
                Err(err) => {
                    warn!("{:?}", err);
                    break;
                }
            }
        }

        Ok(Response::new(PipelineResponse {}))
    }
    async fn get_config(
        &self,
        _request: tonic::Request<GetConfigRequest>,
    ) -> Result<tonic::Response<GetConfigResponse>, tonic::Status> {
        let config_yaml = serde_yaml::to_string(&self.app_config)
            .map_err(|err| tonic::Status::internal(err.to_string()))?;
        Ok(Response::new(GetConfigResponse {
            config_yaml: config_yaml,
        }))
    }
}

pub async fn start_internal_server(
    app_config: Config,
    sender: Sender<PipelineRequest>,
) -> Result<(), tonic::transport::Error> {
    let server = InternalServer {
        sender,
        app_config: app_config.to_owned(),
    };
    let internal_config = app_config.api.internal.unwrap_or(ApiInternal::default());
    let mut addr = format!("{}:{}", internal_config.host, internal_config.port)
        .to_socket_addrs()
        .unwrap();
    Server::builder()
        .add_service(
            internal_grpc::internal_pipeline_service_server::InternalPipelineServiceServer::new(
                server,
            ),
        )
        .serve(addr.next().unwrap())
        .await
}

pub fn start_internal_client(internal_config: ApiInternal, receiver: Receiver<PipelineRequest>) {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut connected = false;
        let mut idx = 0;
        while !connected {
            let addr = format!("http://{}:{}", internal_config.host, internal_config.port);
            if let Ok(mut client) = InternalPipelineServiceClient::connect(addr).await {
                connected = true;
                let iterator = InternalIterator {
                    receiver: receiver.to_owned(),
                };
                let in_stream = tokio_stream::iter(iterator);

                client.stream_pipeline_request(in_stream).await.unwrap();
            } else {
                debug!("waiting to connect to api_server : {}", idx);
                connected = false;
                idx += 1;
                thread::sleep(Duration::from_millis(1200));
            };
        }
    });
}

struct InternalIterator {
    receiver: Receiver<PipelineRequest>,
}
impl Iterator for InternalIterator {
    type Item = PipelineRequest;

    fn next(&mut self) -> Option<Self::Item> {
        match self.receiver.recv() {
            Ok(msg) => Some(msg),
            Err(_) => None,
        }
    }
}
