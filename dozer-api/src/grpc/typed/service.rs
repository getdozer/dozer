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
    errors::{GenerationError, GrpcError},
    generator::protoc::generator::{
        CountResponseDesc, EventDesc, ProtoGenerator, QueryResponseDesc, ServiceDesc,
        TokenResponseDesc,
    },
    grpc::shared_impl,
    RoCacheEndpoint,
};
use dozer_cache::CacheReader;
use dozer_types::{grpc_types::types::Operation, models::api_security::ApiSecurity};
use futures_util::future;
use prost_reflect::{MethodDescriptor, Value};
use std::{borrow::Cow, collections::HashMap, convert::Infallible, path::Path};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    codegen::{self, *},
    metadata::MetadataMap,
    Code, Extensions, Request, Response, Status,
};

#[derive(Debug, Clone)]
struct TypedEndpoint {
    cache_endpoint: Arc<RoCacheEndpoint>,
    service_desc: ServiceDesc,
}

pub struct TypedService {
    accept_compression_encodings: EnabledCompressionEncodings,
    send_compression_encodings: EnabledCompressionEncodings,
    /// For look up endpoint from its full service name. `key == value.service_desc.service.full_name()`.
    endpoint_map: HashMap<String, TypedEndpoint>,
    event_notifier: Option<tokio::sync::broadcast::Receiver<Operation>>,
    security: Option<ApiSecurity>,
}

impl Clone for TypedService {
    fn clone(&self) -> Self {
        Self {
            accept_compression_encodings: self.accept_compression_encodings,
            send_compression_encodings: self.send_compression_encodings,
            endpoint_map: self.endpoint_map.clone(),
            event_notifier: self.event_notifier.as_ref().map(|r| r.resubscribe()),
            security: self.security.to_owned(),
        }
    }
}

impl TypedService {
    pub fn new(
        descriptor_path: &Path,
        cache_endpoints: Vec<Arc<RoCacheEndpoint>>,
        event_notifier: Option<tokio::sync::broadcast::Receiver<Operation>>,
        security: Option<ApiSecurity>,
    ) -> Result<Self, GrpcError> {
        let endpoint_map = cache_endpoints
            .into_iter()
            .map(|cache_endpoint| {
                let service_desc =
                    ProtoGenerator::read_schema(descriptor_path, &cache_endpoint.endpoint.name)?;
                Ok::<_, GenerationError>((
                    service_desc.service.full_name().to_string(),
                    TypedEndpoint {
                        cache_endpoint,
                        service_desc,
                    },
                ))
            })
            .collect::<Result<HashMap<_, _>, _>>()?;
        Ok(Self {
            accept_compression_encodings: EnabledCompressionEncodings::default(),
            send_compression_encodings: EnabledCompressionEncodings::default(),
            endpoint_map,
            event_notifier,
            security,
        })
    }

    fn create_grpc(&self, method_desc: MethodDescriptor) -> tonic::server::Grpc<TypedCodec> {
        tonic::server::Grpc::new(TypedCodec::new(method_desc)).apply_compression_config(
            self.accept_compression_encodings,
            self.send_compression_encodings,
        )
    }

    fn call_impl<B: Body + Send + 'static>(
        &mut self,
        req: http::Request<B>,
    ) -> Option<BoxFuture<http::Response<tonic::body::BoxBody>, Infallible>>
    where
        B::Error: Into<StdError> + Send + 'static,
    {
        // full name will be in the format of `/dozer.generated.users.Users/query`
        let current_path: Vec<&str> = req.uri().path().split('/').collect();
        if current_path.len() != 3 {
            return None;
        }

        let full_service_name = current_path[1];
        let typed_endpoint = self.endpoint_map.get(full_service_name)?;

        let method_name = current_path[2];
        if method_name == typed_endpoint.service_desc.count.method.name() {
            struct CountService {
                cache_endpoint: Arc<RoCacheEndpoint>,
                response_desc: Option<CountResponseDesc>,
            }
            impl tonic::server::UnaryService<DynamicMessage> for CountService {
                type Response = TypedResponse;
                type Future = future::Ready<Result<Response<TypedResponse>, Status>>;
                fn call(&mut self, request: Request<DynamicMessage>) -> Self::Future {
                    let response = count(
                        request,
                        &self.cache_endpoint.cache_reader(),
                        &self.cache_endpoint.endpoint.name,
                        self.response_desc
                            .take()
                            .expect("This future shouldn't be polled twice"),
                    );
                    future::ready(response)
                }
            }

            let mut grpc = self.create_grpc(typed_endpoint.service_desc.count.method.clone());
            let method = CountService {
                cache_endpoint: typed_endpoint.cache_endpoint.clone(),
                response_desc: Some(typed_endpoint.service_desc.count.response_desc.clone()),
            };
            Some(Box::pin(async move {
                let res = grpc.unary(method, req).await;
                Ok(res)
            }))
        } else if method_name == typed_endpoint.service_desc.query.method.name() {
            struct QueryService {
                cache_endpoint: Arc<RoCacheEndpoint>,
                response_desc: Option<QueryResponseDesc>,
            }
            impl tonic::server::UnaryService<DynamicMessage> for QueryService {
                type Response = TypedResponse;
                type Future = future::Ready<Result<Response<TypedResponse>, Status>>;
                fn call(&mut self, request: Request<DynamicMessage>) -> Self::Future {
                    let response = query(
                        request,
                        &self.cache_endpoint.cache_reader(),
                        &self.cache_endpoint.endpoint.name,
                        self.response_desc
                            .take()
                            .expect("This future shouldn't be polled twice"),
                    );
                    future::ready(response)
                }
            }

            let mut grpc = self.create_grpc(typed_endpoint.service_desc.query.method.clone());
            let method = QueryService {
                cache_endpoint: typed_endpoint.cache_endpoint.clone(),
                response_desc: Some(typed_endpoint.service_desc.query.response_desc.clone()),
            };
            Some(Box::pin(async move {
                let res = grpc.unary(method, req).await;
                Ok(res)
            }))
        } else if let Some(on_event_method_desc) = &typed_endpoint.service_desc.on_event {
            if method_name == on_event_method_desc.method.name() {
                struct EventService {
                    cache_endpoint: Arc<RoCacheEndpoint>,
                    event_desc: Option<EventDesc>,
                    event_notifier: Option<tokio::sync::broadcast::Receiver<Operation>>,
                }
                impl tonic::server::ServerStreamingService<DynamicMessage> for EventService {
                    type Response = TypedResponse;

                    type ResponseStream = ReceiverStream<Result<TypedResponse, tonic::Status>>;

                    type Future =
                        future::Ready<Result<tonic::Response<Self::ResponseStream>, tonic::Status>>;
                    fn call(&mut self, request: tonic::Request<DynamicMessage>) -> Self::Future {
                        future::ready(on_event(
                            request,
                            &self.cache_endpoint.cache_reader(),
                            &self.cache_endpoint.endpoint.name,
                            self.event_desc
                                .take()
                                .expect("This future shouldn't be polled twice"),
                            self.event_notifier.take(),
                        ))
                    }
                }

                let mut grpc = self.create_grpc(on_event_method_desc.method.clone());
                let method = EventService {
                    cache_endpoint: typed_endpoint.cache_endpoint.clone(),
                    event_desc: Some(on_event_method_desc.response_desc.clone()),
                    event_notifier: self.event_notifier.as_ref().map(|r| r.resubscribe()),
                };
                Some(Box::pin(async move {
                    let res = grpc.server_streaming(method, req).await;
                    Ok(res)
                }))
            } else {
                None
            }
        } else if let Some(token_method_desc) = &typed_endpoint.service_desc.token {
            if method_name == token_method_desc.method.name() {
                struct AuthService {
                    response_desc: Option<TokenResponseDesc>,
                    security: Option<ApiSecurity>,
                }
                impl tonic::server::UnaryService<DynamicMessage> for AuthService {
                    type Response = TypedResponse;
                    type Future = future::Ready<Result<Response<TypedResponse>, Status>>;
                    fn call(&mut self, request: Request<DynamicMessage>) -> Self::Future {
                        let response = token(
                            request,
                            self.security.take(),
                            self.response_desc
                                .take()
                                .expect("This future shouldn't be polled twice"),
                        );
                        future::ready(response)
                    }
                }

                let mut grpc = self.create_grpc(token_method_desc.method.clone());
                let method = AuthService {
                    response_desc: Some(token_method_desc.response_desc.clone()),
                    security: self.security.clone(),
                };
                Some(Box::pin(async move {
                    let res = grpc.unary(method, req).await;
                    Ok(res)
                }))
            } else {
                None
            }
        } else {
            None
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
        match self.call_impl(req) {
            Some(fut) => fut,
            None => Box::pin(async move {
                Ok(http::Response::builder()
                    .status(200)
                    .header("grpc-status", "12")
                    .header("content-type", "application/grpc")
                    .body(empty_body())
                    .unwrap())
            }),
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
    reader: &CacheReader,
    endpoint_name: &str,
    response_desc: CountResponseDesc,
) -> Result<Response<TypedResponse>, Status> {
    let mut parts = request.into_parts();
    let (query, access) = parse_request(&mut parts)?;

    let count = shared_impl::count(reader, endpoint_name, query.as_deref(), access)?;
    let res = count_response_to_typed_response(count, response_desc);
    Ok(Response::new(res))
}

fn query(
    request: Request<DynamicMessage>,
    reader: &CacheReader,
    endpoint_name: &str,
    response_desc: QueryResponseDesc,
) -> Result<Response<TypedResponse>, Status> {
    let mut parts = request.into_parts();
    let (query, access) = parse_request(&mut parts)?;

    let (_, records) = shared_impl::query(reader, endpoint_name, query.as_deref(), access)?;
    let res = query_response_to_typed_response(records, response_desc);
    Ok(Response::new(res))
}

fn on_event(
    request: Request<DynamicMessage>,
    reader: &CacheReader,
    endpoint_name: &str,
    event_desc: EventDesc,
    event_notifier: Option<tokio::sync::broadcast::Receiver<Operation>>,
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

    let endpoint_to_be_streamed = endpoint_name.to_string();
    shared_impl::on_event(
        reader,
        endpoint_name,
        filter,
        event_notifier,
        access.cloned(),
        move |op| {
            if endpoint_to_be_streamed == op.endpoint_name {
                Some(Ok(on_event_to_typed_response(op, event_desc.clone())))
            } else {
                None
            }
        },
    )
}

fn token(
    request: Request<DynamicMessage>,
    security: Option<ApiSecurity>,
    response_desc: TokenResponseDesc,
) -> Result<Response<TypedResponse>, Status> {
    if let Some(security) = security {
        let _parts = request.into_parts();

        let auth = Authorizer::from(&security);
        let token = auth.generate_token(Access::All, None).unwrap();
        let res = token_response(token, response_desc);
        Ok(Response::new(res))
    } else {
        Err(Status::unavailable("security config unavailable"))
    }
}
