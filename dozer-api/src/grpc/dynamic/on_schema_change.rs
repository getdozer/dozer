use crate::grpc::internal_grpc::pipeline_request::ApiEvent;
use crate::grpc::internal_grpc::PipelineRequest;
use crate::grpc::types::SchemaEvent;
use prost_reflect::DynamicMessage;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{codegen::BoxFuture, Request, Response, Status};

pub struct OnSchemaChangeService {
    pub(crate) event_notifier: tokio::sync::broadcast::Receiver<PipelineRequest>,
}
impl tonic::server::ServerStreamingService<DynamicMessage> for OnSchemaChangeService {
    type Response = SchemaEvent;

    type ResponseStream = ReceiverStream<Result<SchemaEvent, tonic::Status>>;

    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<DynamicMessage>) -> Self::Future {
        let event_notifier = self.event_notifier.resubscribe();
        let fut = async move { on_schema_change_grpc_server_stream(request, event_notifier).await };
        Box::pin(fut)
    }
}

async fn on_schema_change_grpc_server_stream(
    _: Request<DynamicMessage>,
    event_notifier: tokio::sync::broadcast::Receiver<PipelineRequest>,
) -> Result<Response<ReceiverStream<Result<SchemaEvent, tonic::Status>>>, Status> {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    // create subscribe
    let mut broadcast_receiver = event_notifier.resubscribe();
    tokio::spawn(async move {
        while let Ok(event) = broadcast_receiver.recv().await {
            if let Some(ApiEvent::Schema(schema)) = event.api_event {
                if (tx.send(Ok(schema)).await).is_err() {
                    // receiver drop
                    break;
                }
            }
        }
    });
    Ok(Response::new(ReceiverStream::new(rx)))
}
