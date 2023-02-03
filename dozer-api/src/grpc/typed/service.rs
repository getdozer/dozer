use super::{
    codec::TypedCodec,
    helper::{
        count_response_to_typed_response, on_event_to_typed_response,
        query_response_to_typed_response, token_response,
    },
    DynamicMessage, TypedResponse,
};
use crate::{
    auth::{Access, Authorizer},
    grpc::{internal_grpc::PipelineResponse, shared_impl},
    PipelineDetails,
};
use actix_web::http::StatusCode;
use dozer_types::{models::api_security::ApiSecurity, types::Schema};
use futures_util::future;
use inflector::Inflector;
use prost_reflect::{DescriptorPool, Value};
use std::{borrow::Cow, collections::HashMap};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    codegen::{self, *},
    metadata::MetadataMap,
    Code, Extensions, Request, Response, Status,
};
pub struct TypedService {
    accept_compression_encodings: EnabledCompressionEncodings,
    send_compression_encodings: EnabledCompressionEncodings,
    descriptor: DescriptorPool,
    pipeline_map: HashMap<String, PipelineDetails>,
    schema_map: HashMap<String, Schema>,
    event_notifier: Option<tokio::sync::broadcast::Receiver<PipelineResponse>>,
    security: Option<ApiSecurity>,
}

impl Clone for TypedService {
    fn clone(&self) -> Self {
        Self {
            accept_compression_encodings: self.accept_compression_encodings,
            send_compression_encodings: self.send_compression_encodings,
            descriptor: self.descriptor.clone(),
            pipeline_map: self.pipeline_map.clone(),
            schema_map: self.schema_map.clone(),
            event_notifier: self.event_notifier.as_ref().map(|r| r.resubscribe()),
            security: self.security.to_owned(),
        }
    }
}

impl TypedService {
    pub fn new(
        descriptor: DescriptorPool,
        pipeline_map: HashMap<String, PipelineDetails>,
        schema_map: HashMap<String, Schema>,
        event_notifier: Option<tokio::sync::broadcast::Receiver<PipelineResponse>>,
        security: Option<ApiSecurity>,
    ) -> Self {
        TypedService {
            accept_compression_encodings: Default::default(),
            send_compression_encodings: Default::default(),
            descriptor,
            pipeline_map,
            schema_map,
            event_notifier,
            security,
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
        let security = self.security.to_owned();
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
                        endpoint_name.to_lowercase(),
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
                    .unwrap_or_else(|| panic!("gRPC Service not defined: {service_name}"));

                let method_desc = service_desc
                    .methods()
                    .find(|m| m.name() == method_name)
                    .unwrap_or_else(|| panic!("gRPC method not defined: {method_name}"));

                let desc = self.descriptor.clone();
                let accept_compression_encodings = self.accept_compression_encodings;
                let send_compression_encodings = self.send_compression_encodings;
                let codec = TypedCodec::new(method_desc);
                let event_notifier = self.event_notifier.as_ref().map(|r| r.resubscribe());
                let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                    accept_compression_encodings,
                    send_compression_encodings,
                );

                match method_name {
                    "count" => {
                        struct CountService(PipelineDetails, DescriptorPool);
                        impl tonic::server::UnaryService<DynamicMessage> for CountService {
                            type Response = TypedResponse;
                            type Future = future::Ready<Result<Response<TypedResponse>, Status>>;
                            fn call(&mut self, request: Request<DynamicMessage>) -> Self::Future {
                                let response = count(request, &self.0, &self.1);
                                future::ready(response)
                            }
                        }
                        Box::pin(async move {
                            let method = CountService(pipeline_details, desc);
                            let res = grpc.unary(method, req).await;
                            Ok(res)
                        })
                    }
                    "query" => {
                        struct QueryService(PipelineDetails, DescriptorPool);
                        impl tonic::server::UnaryService<DynamicMessage> for QueryService {
                            type Response = TypedResponse;
                            type Future = future::Ready<Result<Response<TypedResponse>, Status>>;
                            fn call(&mut self, request: Request<DynamicMessage>) -> Self::Future {
                                let response = query(request, &self.0, &self.1);
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
                            Option<tokio::sync::broadcast::Receiver<PipelineResponse>>,
                        );
                        impl tonic::server::ServerStreamingService<DynamicMessage> for EventService {
                            type Response = TypedResponse;

                            type ResponseStream =
                                ReceiverStream<Result<TypedResponse, tonic::Status>>;

                            type Future = future::Ready<
                                Result<tonic::Response<Self::ResponseStream>, tonic::Status>,
                            >;
                            fn call(
                                &mut self,
                                request: tonic::Request<DynamicMessage>,
                            ) -> Self::Future {
                                let desc = self.1.clone();
                                let event_notifier = self.2.as_ref().map(|r| r.resubscribe());
                                future::ready(on_event(request, &self.0, desc, event_notifier))
                            }
                        }
                        Box::pin(async move {
                            let method = EventService(pipeline_details, desc, event_notifier);
                            let res = grpc.server_streaming(method, req).await;
                            Ok(res)
                        })
                    }
                    "token" => {
                        struct AuthService(PipelineDetails, DescriptorPool, Option<ApiSecurity>);
                        impl tonic::server::UnaryService<DynamicMessage> for AuthService {
                            type Response = TypedResponse;
                            type Future = future::Ready<Result<Response<TypedResponse>, Status>>;
                            fn call(&mut self, request: Request<DynamicMessage>) -> Self::Future {
                                let pipeline_details = self.0.clone();
                                let desc = self.1.clone();
                                let security = self.2.clone();
                                let response = token(request, security, desc, pipeline_details);
                                future::ready(response)
                            }
                        }
                        Box::pin(async move {
                            // let security = self.security.to_owned();
                            let method = AuthService(pipeline_details, desc, security);
                            let res = grpc.unary(method, req).await;
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

fn parse_request(
    (_, extensions, query_request): &mut (MetadataMap, Extensions, DynamicMessage),
) -> Result<(Option<Cow<str>>, Option<Access>), Status> {
    let access = extensions.remove::<Access>();

    let query = query_request.get_field_by_name("query");
    let query = query
        .map(|query| match query {
            Cow::Owned(query) => {
                if let Value::String(query) = query {
                    Ok(Cow::Owned(query))
                } else {
                    Err(Status::new(Code::InvalidArgument, "query must be a string"))
                }
            }
            Cow::Borrowed(query) => query
                .as_str()
                .map(Cow::Borrowed)
                .ok_or_else(|| Status::new(Code::InvalidArgument, "query must be a string")),
        })
        .transpose()?;
    Ok((query, access))
}

fn count(
    request: Request<DynamicMessage>,
    pipeline_details: &PipelineDetails,
    desc: &DescriptorPool,
) -> Result<Response<TypedResponse>, Status> {
    let mut parts = request.into_parts();
    let (query, access) = parse_request(&mut parts)?;

    let count = shared_impl::count(pipeline_details, query.as_deref(), access)?;
    let res = count_response_to_typed_response(
        count,
        desc,
        &pipeline_details.cache_endpoint.endpoint.name,
    );
    Ok(Response::new(res))
}

fn query(
    request: Request<DynamicMessage>,
    pipeline_details: &PipelineDetails,
    desc: &DescriptorPool,
) -> Result<Response<TypedResponse>, Status> {
    let mut parts = request.into_parts();
    let (query, access) = parse_request(&mut parts)?;

    let (_, records) = shared_impl::query(pipeline_details, query.as_deref(), access)?;
    let res = query_response_to_typed_response(
        records,
        desc,
        &pipeline_details.cache_endpoint.endpoint.name,
    );
    Ok(Response::new(res))
}

fn on_event(
    request: Request<DynamicMessage>,
    pipeline_details: &PipelineDetails,
    desc: DescriptorPool,
    event_notifier: Option<tokio::sync::broadcast::Receiver<PipelineResponse>>,
) -> Result<Response<ReceiverStream<Result<TypedResponse, tonic::Status>>>, Status> {
    let parts = request.into_parts();
    let extensions = parts.1;
    let query_request = parts.2;
    let access = extensions.get::<Access>();
    let filter = query_request.get_field_by_name("filter");
    let filter = filter
        .as_ref()
        .map(|filter| {
            filter
                .as_str()
                .ok_or_else(|| Status::new(Code::InvalidArgument, "filter must be a string"))
        })
        .transpose()?;

    shared_impl::on_event(
        pipeline_details,
        filter,
        event_notifier,
        access.cloned(),
        move |op, endpoint| Some(Ok(on_event_to_typed_response(op, &desc, &endpoint))),
    )
}

fn token(
    request: Request<DynamicMessage>,
    security: Option<ApiSecurity>,
    desc: DescriptorPool,
    pipeline_details: PipelineDetails,
) -> Result<Response<TypedResponse>, Status> {
    if let Some(security) = security {
        let _parts = request.into_parts();
        let endpoint_name = pipeline_details.cache_endpoint.endpoint.name;

        let auth = Authorizer::from(&security);
        let token = auth.generate_token(Access::All, None).unwrap();
        let res = token_response(token, &desc, &endpoint_name);
        Ok(Response::new(res))
    } else {
        Err(Status::unavailable("security config unavailable"))
    }
}
