use super::{
    services::{
        by_id::GetByIdService, list::ListService, on_delete::OnDeleteService,
        on_insert::OnInsertService, on_schema_change::OnSchemaChangeService,
        on_update::OnUpdateService, query::QueryService,
    },
    util::get_proto_descriptor,
};
use crate::{
    api_server::PipelineDetails,
    generator::protoc::proto_service::GrpcType,
    grpc::{
        dynamic_codec::DynamicCodec,
        util::{get_method_by_name, get_service_name},
    },
};
use dozer_types::events::ApiEvent;
use prost_reflect::DescriptorPool;
use std::collections::HashMap;
use tonic::codegen::{self, *};
pub struct TonicServer {
    accept_compression_encodings: EnabledCompressionEncodings,
    send_compression_encodings: EnabledCompressionEncodings,
    descriptor_path: String,
    descriptor: DescriptorPool,
    function_types: HashMap<String, GrpcType>,
    pipeline_map: HashMap<String, PipelineDetails>,
    event_notifier: tokio::sync::broadcast::Receiver<ApiEvent>,
}
impl Clone for TonicServer {
    fn clone(&self) -> Self {
        Self {
            accept_compression_encodings: self.accept_compression_encodings,
            send_compression_encodings: self.send_compression_encodings,
            descriptor_path: self.descriptor_path.clone(),
            descriptor: self.descriptor.clone(),
            function_types: self.function_types.clone(),
            pipeline_map: self.pipeline_map.clone(),
            event_notifier: self.event_notifier.resubscribe(),
        }
    }
}
impl TonicServer {
    pub fn new(
        descriptor_path: String,
        function_types: HashMap<String, GrpcType>,
        pipeline_map: HashMap<String, PipelineDetails>,
        event_notifier: tokio::sync::broadcast::Receiver<ApiEvent>,
    ) -> Self {
        let descriptor = get_proto_descriptor(descriptor_path.to_owned()).unwrap();
        TonicServer {
            accept_compression_encodings: Default::default(),
            send_compression_encodings: Default::default(),
            descriptor_path,
            descriptor,
            function_types,
            pipeline_map,
            event_notifier,
        }
    }

    // #[must_use]
    // pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
    //     self.accept_compression_encodings.enable(encoding);
    //     self
    // }
    // ///Compress responses with the given encoding, if the client supports it.
    // #[must_use]
    // pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
    //     self.send_compression_encodings.enable(encoding);
    //     self
    // }
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
        let service_names = get_service_name(self.descriptor.to_owned());

        let method_name = current_path[current_path.len() - 1];
        let route_match = current_path.len() > 2
            && service_names.contains(&current_path[current_path.len() - 2].to_string());

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
        let method = get_method_by_name(
            self.descriptor.to_owned(),
            current_path[current_path.len() - 2].to_string(),
            method_name.to_string(),
        );
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
            let accept_compression_encodings = self.accept_compression_encodings;
            let send_compression_encodings = self.send_compression_encodings;
            let mut grpc = tonic::server::Grpc::new(codec)
                .apply_compression_config(accept_compression_encodings, send_compression_encodings);
            let pipeline_detail =
                self.pipeline_map[current_path[current_path.len() - 2]].to_owned();
            return match method_type {
                GrpcType::GetById => {
                    let method = GetByIdService {
                        pipeline_details: pipeline_detail,
                    };
                    let fut = async move {
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                GrpcType::Query => {
                    let method = QueryService {
                        pipeline_details: pipeline_detail,
                    };
                    let fut = async move {
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                GrpcType::List => {
                    let method = ListService {
                        pipeline_details: pipeline_detail,
                    };
                    let fut = async move {
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                GrpcType::OnInsert => {
                    let method = OnInsertService {
                        pipeline_details: pipeline_detail,
                        event_notifier: self.event_notifier.resubscribe(),
                    };
                    let fut = async move {
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                GrpcType::OnUpdate => {
                    let method = OnUpdateService {
                        pipeline_details: pipeline_detail,
                        event_notifier: self.event_notifier.resubscribe(),
                    };
                    let fut = async move {
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                GrpcType::OnDelete => {
                    let method = OnDeleteService {
                        pipeline_details: pipeline_detail,
                        event_notifier: self.event_notifier.resubscribe(),
                    };
                    let fut = async move {
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                GrpcType::OnSchemaChange => {
                    let method = OnSchemaChangeService {
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
