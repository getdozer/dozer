#![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
use std::collections::HashMap;

use crate::{
    generator::protoc::proto_service::GrpcType,
    grpc::{
        dynamic_codec::MyCodec,
        services::{GetByIdService, ListService, QueryService},
        util::get_method_by_name,
    },
    proto_util::get_proto_descriptor,
};
use dozer_cache::cache::LmdbCache;
use prost_reflect::DescriptorPool;
use tonic::{
    codegen::{self, *},
};
/// The greeting service definition.
pub struct GRPCServer {
    accept_compression_encodings: EnabledCompressionEncodings,
    send_compression_encodings: EnabledCompressionEncodings,
    descriptor_path: String,
    descriptor: DescriptorPool,
    function_types: HashMap<String, GrpcType>,
    cache: Arc<LmdbCache>,
}
impl GRPCServer {
    pub fn new(
        descriptor_path: String,
        function_types: HashMap<String, GrpcType>,
        cache: Arc<LmdbCache>,
    ) -> Self {
        let descriptor = get_proto_descriptor(descriptor_path.to_owned()).unwrap();
        return GRPCServer {
            accept_compression_encodings: Default::default(),
            send_compression_encodings: Default::default(),
            descriptor_path,
            descriptor,
            function_types,
            cache,
        };
    }
    // pub fn with_interceptor<F>(
    //     inner: T,
    //     interceptor: F,
    // ) -> InterceptedService<Self, F>
    // where
    //     F: tonic::service::Interceptor,
    // {
    //     InterceptedService::new(Self::new(inner), interceptor)
    // }
    ///Enable decompressing requests with the given encoding.
    #[must_use]
    pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
        self.accept_compression_encodings.enable(encoding);
        self
    }
    ///Compress responses with the given encoding, if the client supports it.
    #[must_use]
    pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
        self.send_compression_encodings.enable(encoding);
        self
    }
}
impl<B> codegen::Service<http::Request<B>> for GRPCServer
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        let current_path: Vec<&str> = req.uri().path().split("/").collect();
        let method_name = current_path[current_path.len() - 1];
        let method = get_method_by_name(self.descriptor.to_owned(), method_name.to_string());
        if let Some(method_descriptor) = method {
            let input = method_descriptor.input();
            let output = method_descriptor.output();
            let full_name_input = input.full_name();
            let full_name_output = output.full_name();
            let codec = MyCodec::new(
                String::from(full_name_input.to_owned()),
                String::from(full_name_output.to_owned()),
                self.descriptor_path.to_owned(),
            );
            let method_type = &self.function_types[method_name];
            let cache = self.cache.to_owned();
            #[allow(non_camel_case_types)]
            let accept_compression_encodings = self.accept_compression_encodings;
            let send_compression_encodings = self.send_compression_encodings;
            let descriptor_path = self.descriptor_path.to_owned();
            let mut grpc = tonic::server::Grpc::new(codec)
                .apply_compression_config(accept_compression_encodings, send_compression_encodings);
            match method_type {
                GrpcType::GetById => {
                    let method = GetByIdService {
                        cache: cache.to_owned(),
                    };
                    let fut = async move {
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    return Box::pin(fut);
                }
                GrpcType::Query => {
                    let method = QueryService { cache };
                    let fut = async move {
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    return Box::pin(fut);
                }
                GrpcType::List => {
                    let method = ListService { cache };
                    let fut = async move {
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    return Box::pin(fut);
                }
            }
        } else {
            Box::pin(async move {
                Ok(http::Response::builder()
                    .status(200)
                    .header("grpc-status", "12")
                    .header("content-type", "application/grpc")
                    .body(empty_body())
                    .unwrap())
            })
        }
    }
}
impl Clone for GRPCServer {
    fn clone(&self) -> Self {
        Self {
            accept_compression_encodings: self.accept_compression_encodings,
            send_compression_encodings: self.send_compression_encodings,
            descriptor_path: self.descriptor_path.to_owned(),
            descriptor: self.descriptor.to_owned(),
            function_types: self.function_types.to_owned(),
            cache: self.cache.to_owned(),
        }
    }
}
fn get_name() -> &'static str {
    return "abc";
}
impl tonic::server::NamedService for GRPCServer {
    const NAME: &'static str = "Dozer.Service";
}
