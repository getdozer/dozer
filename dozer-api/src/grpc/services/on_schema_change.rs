use crate::errors::GRPCError;
use dozer_types::serde_json::{self, Map};
use dozer_types::{events::ApiEvent, serde_json::Value};
use prost_reflect::DynamicMessage;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{codegen::BoxFuture, Request, Response, Status};

pub struct OnSchemaChangeService {
    pub(crate) event_notifier: tokio::sync::broadcast::Receiver<ApiEvent>,
}
impl tonic::server::ServerStreamingService<DynamicMessage> for OnSchemaChangeService {
    type Response = Value;

    type ResponseStream = ReceiverStream<Result<Value, tonic::Status>>;

    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<DynamicMessage>) -> Self::Future {
        let event_notifier = self.event_notifier.resubscribe();
        let fut = async move { on_schema_change_grpc_server_stream(request, event_notifier).await };
        Box::pin(fut)
    }
}

async fn on_schema_change_grpc_server_stream(
    _: Request<DynamicMessage>,
    event_notifier: tokio::sync::broadcast::Receiver<ApiEvent>,
) -> Result<Response<ReceiverStream<Result<Value, tonic::Status>>>, Status> {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    // create subscribe
    let mut broadcast_receiver = event_notifier.resubscribe();
    tokio::spawn(async move {
        while let Ok(event) = broadcast_receiver.recv().await {
            if let ApiEvent::SchemaChange(schema) = event {
                let value_json = serde_json::to_value(schema)
                    .map_err(GRPCError::SerizalizeError)
                    .unwrap();
                // wrap to object
                let mut on_change_response: Map<String, Value> = Map::new();
                on_change_response.insert("detail".to_owned(), value_json);
                let result_respone = serde_json::to_value(on_change_response).unwrap();
                if (tx.send(Ok(result_respone)).await).is_err() {
                    // receiver drop
                    break;
                }
            }
        }
    });
    Ok(Response::new(ReceiverStream::new(rx)))
}
