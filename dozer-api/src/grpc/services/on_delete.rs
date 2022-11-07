use crate::api_helper;
use crate::api_server::PipelineDetails;
use crate::errors::GRPCError;
use dozer_cache::cache::LmdbCache;
use dozer_types::serde_json::{self, Map};
use dozer_types::{events::Event, serde_json::Value};
use prost_reflect::DynamicMessage;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{codegen::BoxFuture, Request, Response, Status};

pub struct OnDeleteService {
    pub(crate) cache: Arc<LmdbCache>,
    pub(crate) pipeline_details: PipelineDetails,
    pub(crate) event_notifier: tokio::sync::broadcast::Receiver<Event>,
}
impl tonic::server::ServerStreamingService<DynamicMessage> for OnDeleteService {
    type Response = Value;

    type ResponseStream = ReceiverStream<Result<Value, tonic::Status>>;

    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<DynamicMessage>) -> Self::Future {
        let cache = self.cache.to_owned();
        let pipeline_details = self.pipeline_details.to_owned();
        let event_notifier = self.event_notifier.resubscribe();
        let fut = async move {
            on_delete_grpc_server_stream(
                pipeline_details.to_owned(),
                cache.to_owned(),
                request,
                event_notifier,
            )
            .await
        };
        Box::pin(fut)
    }
}

async fn on_delete_grpc_server_stream(
    pipeline_details: PipelineDetails,
    cache: Arc<LmdbCache>,
    _: Request<DynamicMessage>,
    event_notifier: tokio::sync::broadcast::Receiver<Event>,
) -> Result<Response<ReceiverStream<Result<Value, tonic::Status>>>, Status> {
    let api_helper = api_helper::ApiHelper::new(pipeline_details, cache, None)?;
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    // create subscribe
    let mut broadcast_receiver = event_notifier.resubscribe();
    tokio::spawn(async move {
        while let Ok(event) = broadcast_receiver.recv().await {
            if let Event::RecordDelete(record) = event {
                let converted_record = api_helper.convert_record_to_json(record).unwrap();
                let value_json = serde_json::to_value(converted_record)
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
