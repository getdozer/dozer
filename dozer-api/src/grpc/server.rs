#![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
use super::{services::StreamingService, util::get_proto_descriptor};
use crate::{
    api_server::PipelineDetails,
    generator::protoc::proto_service::GrpcType,
    grpc::{
        dynamic_codec::DynamicCodec,
        services::{GetByIdService, ListService, QueryService},
        util::{get_method_by_name, get_service_name},
    },
};
use dozer_cache::cache::LmdbCache;
use dozer_types::events::Event;
use prost_reflect::DescriptorPool;
use std::collections::HashMap;
use tonic::codegen::{self, *};
pub struct TonicServer {
    accept_compression_encodings: EnabledCompressionEncodings,
    send_compression_encodings: EnabledCompressionEncodings,
    descriptor_path: String,
    descriptor: DescriptorPool,
    function_types: HashMap<String, GrpcType>,
    cache: Arc<LmdbCache>,
    pipeline_details: PipelineDetails,
    //event_notifier: crossbeam::channel::Receiver<Event>
    event_notifier: tokio::sync::broadcast::Receiver<Event>,
}
impl Clone for TonicServer {
    fn clone(&self) -> Self {
        Self {
            accept_compression_encodings: self.accept_compression_encodings.clone(),
            send_compression_encodings: self.send_compression_encodings.clone(),
            descriptor_path: self.descriptor_path.clone(),
            descriptor: self.descriptor.clone(),
            function_types: self.function_types.clone(),
            cache: self.cache.clone(),
            pipeline_details: self.pipeline_details.clone(),
            event_notifier: self.event_notifier.resubscribe(),
        }
    }
}
impl TonicServer {
    pub fn new(
        descriptor_path: String,
        function_types: HashMap<String, GrpcType>,
        cache: Arc<LmdbCache>,
        pipeline_details: PipelineDetails,
        //event_notifier: crossbeam::channel::Receiver<Event>
        event_notifier: tokio::sync::broadcast::Receiver<Event>,
    ) -> Self {
        let descriptor = get_proto_descriptor(descriptor_path.to_owned()).unwrap();
        TonicServer {
            accept_compression_encodings: Default::default(),
            send_compression_encodings: Default::default(),
            descriptor_path,
            descriptor,
            function_types,
            cache,
            pipeline_details,
            event_notifier,
        }
    }

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
impl<B> codegen::Service<http::Request<B>> for TonicServer
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
        let current_path: Vec<&str> = req.uri().path().split('/').collect();
        let service_name = get_service_name(self.descriptor.to_owned());
        let method_name = current_path[current_path.len() - 1];
        let method = get_method_by_name(self.descriptor.to_owned(), method_name.to_string());
        let route_match = current_path.len() > 2
            && Some(current_path[current_path.len() - 2].to_string()) == service_name;
        if !route_match {
            return Box::pin(async move {
                Ok(http::Response::builder()
                    .status(200)
                    .header("grpc-status", "12")
                    .header("content-type", "application/grpc")
                    .body(empty_body())
                    .unwrap())
            });
        }
        if let Some(method_descriptor) = method {
            let input = method_descriptor.input();
            let output = method_descriptor.output();
            let full_name_input = input.full_name();
            let full_name_output = output.full_name();
            let codec = DynamicCodec::new(
                full_name_input.to_owned(),
                full_name_output.to_owned(),
                self.descriptor_path.to_owned(),
            );
            let method_type = &self.function_types[method_name];
            let cache = self.cache.to_owned();
            let schema_name = self.pipeline_details.schema_name.to_owned();
            #[allow(non_camel_case_types)]
            let accept_compression_encodings = self.accept_compression_encodings;
            let send_compression_encodings = self.send_compression_encodings;
            let descriptor_path = self.descriptor_path.to_owned();
            let mut grpc = tonic::server::Grpc::new(codec)
                .apply_compression_config(accept_compression_encodings, send_compression_encodings);
            return match method_type {
                GrpcType::GetById => {
                    let method = GetByIdService {
                        cache,
                        pipeline_details: self.pipeline_details.to_owned(),
                    };
                    let fut = async move {
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                GrpcType::Query => {
                    let method = QueryService {
                        cache,
                        pipeline_details: self.pipeline_details.to_owned(),
                    };
                    let fut = async move {
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                GrpcType::List => {
                    let method = ListService {
                        cache,
                        pipeline_details: self.pipeline_details.to_owned(),
                    };
                    let fut = async move {
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                GrpcType::ServerStreaming => {
                    let method = StreamingService {
                        cache,
                        pipeline_details: self.pipeline_details.to_owned(),
                        event_notifier: self.event_notifier.resubscribe(),
                    };
                    let fut = async move {
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
            };
        }
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

impl tonic::server::NamedService for TonicServer {
    const NAME: &'static str = ":package.servicename";
}
