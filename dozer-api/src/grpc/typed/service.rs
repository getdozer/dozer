use super::{
    codec::TypedCodec,
    helper::{on_event_to_typed_response, query_response_to_typed_response},
    DynamicMessage, TypedResponse,
};
use crate::{
    grpc::{
        internal_grpc::{pipeline_request::ApiEvent, PipelineRequest},
        shared_impl,
    },
    PipelineDetails,
};
use actix_web::http::StatusCode;
use dozer_types::types::Schema;
use futures_util::future;
use inflector::Inflector;
use prost_reflect::DescriptorPool;
use std::collections::HashMap;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    codegen::{self, *},
    Code, Request, Response, Status,
};
pub struct TypedService {
    accept_compression_encodings: EnabledCompressionEncodings,
    send_compression_encodings: EnabledCompressionEncodings,
    descriptor: DescriptorPool,
    pipeline_map: HashMap<String, PipelineDetails>,
    schema_map: HashMap<String, Schema>,
    event_notifier: tokio::sync::broadcast::Receiver<PipelineRequest>,
}
impl Clone for TypedService {
    fn clone(&self) -> Self {
        Self {
            accept_compression_encodings: self.accept_compression_encodings,
            send_compression_encodings: self.send_compression_encodings,
            descriptor: self.descriptor.clone(),
            pipeline_map: self.pipeline_map.clone(),
            schema_map: self.schema_map.clone(),
            event_notifier: self.event_notifier.resubscribe(),
        }
    }
}

impl TypedService {
    pub fn new(
        descriptor: DescriptorPool,
        pipeline_map: HashMap<String, PipelineDetails>,
        schema_map: HashMap<String, Schema>,
        event_notifier: tokio::sync::broadcast::Receiver<PipelineRequest>,
    ) -> Self {
        TypedService {
            accept_compression_encodings: Default::default(),
            send_compression_encodings: Default::default(),
            descriptor,
            pipeline_map,
            schema_map,
            event_notifier,
        }
    }
}
impl<B> codegen::Service<http::Request<B>> for TypedService
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
        // full name will be in the format of `/dozer.generated.users.Users/query`

        let current_path: Vec<&str> = req.uri().path().split('/').collect();

        let method_name = current_path[current_path.len() - 1];

        let req_service_name = current_path[current_path.len() - 2];

        let service_tuple = self
            .pipeline_map
            .clone()
            .into_iter()
            .map(|(endpoint_name, details)| {
                (
                    format!(
                        "dozer.generated.{}.{}",
                        endpoint_name.to_lowercase().to_plural(),
                        endpoint_name.to_pascal_case().to_plural()
                    ),
                    details,
                )
            })
            .find(|(service_name, _)| current_path.len() > 2 && req_service_name == service_name);

        match service_tuple {
            None => Box::pin(async move {
                Ok(http::Response::builder()
                    .status(200)
                    .header("grpc-status", "12")
                    .header("content-type", "application/grpc")
                    .body(empty_body())
                    .unwrap())
            }),
            Some((service_name, pipeline_details)) => {
                let service_desc = self
                    .descriptor
                    .services()
                    .find(|s| s.full_name() == service_name)
                    .unwrap_or_else(|| panic!("gRPC Service not defined: {}", service_name));

                let method_desc = service_desc
                    .methods()
                    .find(|m| m.name() == method_name)
                    .unwrap_or_else(|| panic!("gRPC method not defined: {}", method_name));

                let desc = self.descriptor.clone();
                let accept_compression_encodings = self.accept_compression_encodings;
                let send_compression_encodings = self.send_compression_encodings;
                let codec = TypedCodec::new(method_desc);
                let event_notifier = self.event_notifier.resubscribe();
                let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                    accept_compression_encodings,
                    send_compression_encodings,
                );

                match method_name {
                    "query" => {
                        struct QueryService(PipelineDetails, DescriptorPool);

                        impl tonic::server::UnaryService<DynamicMessage> for QueryService {
                            type Response = TypedResponse;
                            type Future = future::Ready<Result<Response<TypedResponse>, Status>>;
                            fn call(&mut self, request: Request<DynamicMessage>) -> Self::Future {
                                let pipeline_details = self.0.clone();
                                let desc = self.1.clone();
                                let response = query(request, pipeline_details, desc);
                                future::ready(response)
                            }
                        }
                        Box::pin(async move {
                            let method = QueryService(pipeline_details, desc);
                            let res = grpc.unary(method, req).await;
                            Ok(res)
                        })
                    }

                    "on_event" => {
                        struct EventService(
                            PipelineDetails,
                            DescriptorPool,
                            tokio::sync::broadcast::Receiver<PipelineRequest>,
                        );

                        impl tonic::server::ServerStreamingService<DynamicMessage> for EventService {
                            type Response = TypedResponse;

                            type ResponseStream =
                                ReceiverStream<Result<TypedResponse, tonic::Status>>;

                            type Future =
                                BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                            fn call(
                                &mut self,
                                request: tonic::Request<DynamicMessage>,
                            ) -> Self::Future {
                                let pipeline_details = self.0.to_owned();
                                let desc = self.1.clone();
                                let event_notifier = self.2.resubscribe();
                                let fut = async move {
                                    on_event(
                                        request,
                                        pipeline_details.to_owned(),
                                        desc,
                                        event_notifier,
                                    )
                                    .await
                                };
                                Box::pin(fut)
                            }
                        }
                        Box::pin(async move {
                            let method = EventService(pipeline_details, desc, event_notifier);
                            let res = grpc.server_streaming(method, req).await;
                            Ok(res)
                        })
                    }
                    _ => Box::pin(async move {
                        Ok(http::Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .header("grpc-status", "12")
                            .header("content-type", "application/grpc")
                            .body(empty_body())
                            .unwrap())
                    }),
                }
            }
        }
    }
}

impl tonic::server::NamedService for TypedService {
    const NAME: &'static str = ":dozer.generated";
}

fn query(
    request: Request<DynamicMessage>,
    pipeline_details: PipelineDetails,
    desc: DescriptorPool,
) -> Result<Response<TypedResponse>, Status> {
    let endpoint_name = pipeline_details.cache_endpoint.endpoint.name.clone();
    let req = request.into_inner();
    let query = req.get_field_by_name("query");
    let query = query
        .as_ref()
        .map(|query| {
            query
                .as_str()
                .ok_or_else(|| Status::new(Code::InvalidArgument, "query must be a string"))
        })
        .transpose()?;

    let (schema, records) = shared_impl::query(pipeline_details, query)?;
    let res = query_response_to_typed_response(records, schema, desc, endpoint_name);
    Ok(Response::new(res))
}

async fn on_event(
    _req: Request<DynamicMessage>,
    pipeline_details: PipelineDetails,
    desc: DescriptorPool,
    event_notifier: tokio::sync::broadcast::Receiver<PipelineRequest>,
) -> Result<Response<ReceiverStream<Result<TypedResponse, tonic::Status>>>, Status> {
    let endpoint_name = pipeline_details.cache_endpoint.endpoint.name;

    let (tx, rx) = tokio::sync::mpsc::channel(1);
    // create subscribe
    let mut broadcast_receiver = event_notifier.resubscribe();
    tokio::spawn(async move {
        while let Ok(event) = broadcast_receiver.recv().await {
            if let Some(ApiEvent::Op(op)) = event.api_event {
                let event =
                    on_event_to_typed_response(op, desc.to_owned(), endpoint_name.to_owned());
                if tx.send(Ok(event)).await.is_err() {
                    // receiver dropped
                    break;
                }
            }
        }
    });
    Ok(Response::new(ReceiverStream::new(rx)))
}
