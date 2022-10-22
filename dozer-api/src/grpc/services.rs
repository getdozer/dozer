use std::sync::Arc;

use super::functions::{grpc_get_by_id, grpc_list, grpc_query};
use dozer_cache::cache::LmdbCache;
use prost_reflect::DynamicMessage;
use serde_json::Value;
use tonic::codegen::BoxFuture;

pub struct ListService {
    pub(crate) cache: Arc<LmdbCache>,
}
impl ListService {}
impl tonic::server::UnaryService<DynamicMessage> for ListService {
    type Response = Value;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call<'a>(&mut self, request: tonic::Request<DynamicMessage>) -> Self::Future {
        let cache = self.cache.to_owned();
        let fut = async move { grpc_list(cache.to_owned(), request).await };
        Box::pin(fut)
    }
}

pub struct GetByIdService {
    pub(crate) cache: Arc<LmdbCache>,
}
impl tonic::server::UnaryService<DynamicMessage> for GetByIdService {
    type Response = Value;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<DynamicMessage>) -> Self::Future {
        let cache = self.cache.to_owned();
        let fut = async move { grpc_get_by_id(cache.to_owned(), request).await };
        Box::pin(fut)
    }
}

pub struct QueryService {
    pub(crate) cache: Arc<LmdbCache>,
}
impl tonic::server::UnaryService<DynamicMessage> for QueryService {
    type Response = Value;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<DynamicMessage>) -> Self::Future {
        let cache = self.cache.to_owned();
        let fut = async move { grpc_query(cache.to_owned(), request).await };
        Box::pin(fut)
    }
}
