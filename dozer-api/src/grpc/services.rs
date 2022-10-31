use super::functions::{grpc_get_by_id, grpc_list, grpc_query};
use crate::{api_server::PipelineDetails, grpc::functions::grpc_server_stream};
use dozer_cache::cache::LmdbCache;
use dozer_types::{events::Event, serde_json::Value};
use prost_reflect::DynamicMessage;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::BoxFuture;

pub struct ListService {
    pub(crate) cache: Arc<LmdbCache>,
    pub(crate) pipeline_details: PipelineDetails,
}
impl ListService {}
impl tonic::server::UnaryService<DynamicMessage> for ListService {
    type Response = Value;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call<'a>(&mut self, request: tonic::Request<DynamicMessage>) -> Self::Future {
        let cache = self.cache.to_owned();
        let pipeline_details = self.pipeline_details.to_owned();
        let fut = async move { grpc_list(pipeline_details, cache.to_owned(), request).await };
        Box::pin(fut)
    }
}

pub struct GetByIdService {
    pub(crate) cache: Arc<LmdbCache>,
    pub(crate) pipeline_details: PipelineDetails,
}
impl tonic::server::UnaryService<DynamicMessage> for GetByIdService {
    type Response = Value;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<DynamicMessage>) -> Self::Future {
        let cache = self.cache.to_owned();
        let pipeline_details = self.pipeline_details.to_owned();
        let fut = async move {
            grpc_get_by_id(pipeline_details.to_owned(), cache.to_owned(), request).await
        };
        Box::pin(fut)
    }
}

pub struct QueryService {
    pub(crate) cache: Arc<LmdbCache>,
    pub(crate) pipeline_details: PipelineDetails,
}
impl tonic::server::UnaryService<DynamicMessage> for QueryService {
    type Response = Value;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<DynamicMessage>) -> Self::Future {
        let cache = self.cache.to_owned();
        let pipeline_details = self.pipeline_details.to_owned();
        let fut =
            async move { grpc_query(pipeline_details.to_owned(), cache.to_owned(), request).await };
        Box::pin(fut)
    }
}

pub struct StreamingService {
    pub(crate) cache: Arc<LmdbCache>,
    pub(crate) pipeline_details: PipelineDetails,
    pub(crate) event_notifier: tokio::sync::broadcast::Receiver<Event> //crossbeam::channel::Receiver<Event>,
}
impl tonic::server::ServerStreamingService<DynamicMessage> for StreamingService {
    type Response = Value;

    type ResponseStream = ReceiverStream<Result<Value, tonic::Status>>;

    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<DynamicMessage>) -> Self::Future {
        let cache = self.cache.to_owned();
        let pipeline_details = self.pipeline_details.to_owned();
        let event_notifier = self.event_notifier.resubscribe();
        let fut = async move {
            grpc_server_stream(
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
