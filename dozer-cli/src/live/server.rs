use std::sync::Arc;

use dozer_api::tonic_web;
use dozer_types::grpc_types::live::{
    code_service_server::{CodeService, CodeServiceServer},
    CommonRequest, DotResponse, LiveResponse, SchemasResponse, SourcesRequest,
};
use futures::stream::BoxStream;
use tokio::sync::broadcast::Receiver;

use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use super::state::LiveState;
pub struct LiveServer {
    pub receiver: Receiver<LiveResponse>,
    pub state: Arc<LiveState>,
}

#[tonic::async_trait]
impl CodeService for LiveServer {
    type LiveConnectStream = BoxStream<'static, Result<LiveResponse, Status>>;
    async fn live_connect(
        &self,
        _request: Request<CommonRequest>,
    ) -> Result<Response<Self::LiveConnectStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let mut receiver = self.receiver.resubscribe();
        let initial_state = self.state.clone();
        tokio::spawn(async move {
            let initial_state = initial_state.get_current();
            match initial_state {
                Ok(initial_state) => {
                    if let Err(e) = tx.send(Ok(initial_state)).await {
                        println!("Error getting initial state");
                        println!("{:?}", e);
                    }
                }
                Err(e) => {
                    println!("Error sending to channel");
                    println!("{:?}", e);
                }
            };

            loop {
                let res = receiver.recv().await;
                match res {
                    Ok(res) => {
                        let res = tx.send(Ok(res)).await;
                        match res {
                            Ok(_) => {}
                            Err(e) => {
                                println!("Error sending to channel");
                                println!("{:?}", e);
                            }
                        }
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        });
        let stream = ReceiverStream::new(rx);

        Ok(Response::new(Box::pin(stream) as Self::LiveConnectStream))
    }

    async fn sources(
        &self,
        request: Request<SourcesRequest>,
    ) -> Result<Response<SchemasResponse>, Status> {
        let req = request.into_inner();
        let res = self.state.get_source_schemas(req.connection_name).await;
        match res {
            Ok(res) => Ok(Response::new(res)),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn endpoints(
        &self,
        _request: Request<CommonRequest>,
    ) -> Result<Response<SchemasResponse>, Status> {
        let state = self.state.clone();
        let handle = std::thread::spawn(move || state.get_endpoints_schemas());
        let res = handle.join().unwrap();

        match res {
            Ok(res) => Ok(Response::new(res)),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn generate_dot(
        &self,
        _request: Request<CommonRequest>,
    ) -> Result<Response<DotResponse>, Status> {
        let state = self.state.clone();
        let handle = std::thread::spawn(move || state.generate_dot());
        let res = handle.join().unwrap();

        match res {
            Ok(res) => Ok(Response::new(res)),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}

pub async fn serve(receiver: Receiver<LiveResponse>, state: Arc<LiveState>) {
    let addr = "0.0.0.0:4556".parse().unwrap();
    let live_server = LiveServer { receiver, state };
    let svc = CodeServiceServer::new(live_server);

    // Enable CORS for local development
    let svc = tonic_web::config().allow_all_origins().enable(svc);

    tonic::transport::Server::builder()
        .accept_http1(true)
        .add_service(svc)
        .serve(addr)
        .await
        .unwrap();
}
