use super::functions::{grpc_get_by_id, grpc_list, grpc_query};
use dozer_cache::cache::LmdbCache;
use dozer_types::serde_json::Value;
use prost_reflect::DynamicMessage;
use std::sync::Arc;
use tonic::codegen::BoxFuture;

pub struct ListService {
    pub(crate) cache: Arc<LmdbCache>,
    pub(crate) schema_name: String,
}
impl ListService {}
impl tonic::server::UnaryService<DynamicMessage> for ListService {
    type Response = Value;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call<'a>(&mut self, request: tonic::Request<DynamicMessage>) -> Self::Future {
        let cache = self.cache.to_owned();
        let schema_name = self.schema_name.to_owned();
        let fut = async move { grpc_list(schema_name, cache.to_owned(), request).await };
        Box::pin(fut)
    }
}

pub struct GetByIdService {
    pub(crate) cache: Arc<LmdbCache>,
    pub(crate) schema_name: String,
}
impl tonic::server::UnaryService<DynamicMessage> for GetByIdService {
    type Response = Value;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<DynamicMessage>) -> Self::Future {
        let cache = self.cache.to_owned();
        let schema_name = self.schema_name.to_owned();
        let fut = async move { grpc_get_by_id(schema_name, cache.to_owned(), request).await };
        Box::pin(fut)
    }
}

pub struct QueryService {
    pub(crate) cache: Arc<LmdbCache>,
    pub(crate) schema_name: String,
}
impl tonic::server::UnaryService<DynamicMessage> for QueryService {
    type Response = Value;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<DynamicMessage>) -> Self::Future {
        let cache = self.cache.to_owned();
        let schema_name = self.schema_name.to_owned();
        let fut = async move { grpc_query(schema_name, cache.to_owned(), request).await };
        Box::pin(fut)
    }
}
