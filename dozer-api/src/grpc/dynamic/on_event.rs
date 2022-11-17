use crate::grpc::internal_grpc::pipeline_request::ApiEvent;
use crate::grpc::internal_grpc::PipelineRequest;
use crate::grpc::types::OperationType;
use crate::PipelineDetails;
use crate::{api_helper, grpc::types::Operation};

use prost_reflect::DynamicMessage;

use tokio_stream::wrappers::ReceiverStream;
use tonic::{codegen::BoxFuture, Request, Response, Status};

pub struct OnDeleteService {
    pub(crate) pipeline_details: PipelineDetails,
    pub(crate) event_notifier: tokio::sync::broadcast::Receiver<PipelineRequest>,
}
impl tonic::server::ServerStreamingService<DynamicMessage> for OnDeleteService {
    type Response = Operation;

    type ResponseStream = ReceiverStream<Result<Operation, tonic::Status>>;

    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<DynamicMessage>) -> Self::Future {
        let pipeline_details = self.pipeline_details.to_owned();
        let event_notifier = self.event_notifier.resubscribe();
        let fut = async move {
            on_delete_grpc_server_stream(pipeline_details.to_owned(), request, event_notifier).await
        };
        Box::pin(fut)
    }
}

async fn on_delete_grpc_server_stream(
    pipeline_details: PipelineDetails,
    req: Request<DynamicMessage>,
    event_notifier: tokio::sync::broadcast::Receiver<PipelineRequest>,
) -> Result<Response<ReceiverStream<Result<Operation, tonic::Status>>>, Status> {
    let api_helper = api_helper::ApiHelper::new(pipeline_details, None)?;
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    // create subscribe
    let mut broadcast_receiver = event_notifier.resubscribe();
    tokio::spawn(async move {
        while let Ok(event) = broadcast_receiver.recv().await {
            if let Some(ApiEvent::Op(op)) = event.api_event {
                if op.typ == OperationType::Delete as i32 {
                    if (tx.send(Ok(op)).await).is_err() {
                        // receiver drop
                        break;
                    }
                }
            }
        }
    });
    Ok(Response::new(ReceiverStream::new(rx)))
}
