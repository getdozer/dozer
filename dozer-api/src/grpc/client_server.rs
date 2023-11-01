use crate::api_helper::get_api_security;
use crate::errors::ApiInitError;
use crate::grpc::auth::AuthService;
use crate::grpc::grpc_web_middleware::enable_grpc_web;
use crate::grpc::health::HealthService;
use crate::grpc::metric_middleware::MetricMiddlewareLayer;
use crate::grpc::{
    auth_middleware::AuthMiddlewareLayer, common::CommonService, typed::TypedService,
};
use crate::grpc::{common, run_server, typed};
use crate::shutdown::ShutdownReceiver;
use crate::{errors::GrpcError, CacheEndpoint};

use dozer_tracing::LabelsAndProgress;

use dozer_services::{
    auth::auth_grpc_service_server::AuthGrpcServiceServer,
    common::common_grpc_service_server::CommonGrpcServiceServer,
    health::{
        health_check_response::ServingStatus, health_grpc_service_server::HealthGrpcServiceServer,
    },
    types::Operation,
};

use dozer_services::tonic::transport::{server::TcpIncoming, Server};
use dozer_types::models::{
    api_config::{default_grpc_port, default_host},
    flags::default_dynamic,
};
use dozer_types::{
    log::info,
    models::{api_config::GrpcApiOptions, api_security::ApiSecurity, flags::Flags},
    tracing::Level,
};

use futures_util::Future;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::broadcast::{self, Receiver};
use tonic_reflection::server::{ServerReflection, ServerReflectionServer};
use tower::Layer;
use tower_http::trace::{self, TraceLayer};

pub struct ApiServer {
    port: u16,
    host: String,
    security: Option<ApiSecurity>,
    flags: Flags,
}

impl ApiServer {
    fn get_dynamic_service(
        &self,
        cache_endpoints: Vec<Arc<CacheEndpoint>>,
        operations_receiver: Option<broadcast::Receiver<Operation>>,
        default_max_num_records: usize,
    ) -> Result<
        (
            Option<TypedService>,
            ServerReflectionServer<impl ServerReflection>,
        ),
        ApiInitError,
    > {
        let mut all_descriptor_bytes = vec![];
        for cache_endpoint in &cache_endpoints {
            all_descriptor_bytes.push(cache_endpoint.descriptor().to_vec());
        }

        let mut builder = tonic_reflection::server::Builder::configure();
        for descriptor_bytes in &all_descriptor_bytes {
            builder = builder.register_encoded_file_descriptor_set(descriptor_bytes);
        }
        let inflection_service = builder.build().map_err(GrpcError::ServerReflectionError)?;
        let security = get_api_security(self.security.to_owned());

        // Service handling dynamic gRPC requests.
        let typed_service = if self.flags.dynamic.unwrap_or_else(default_dynamic) {
            Some(TypedService::new(
                cache_endpoints,
                operations_receiver,
                security,
                default_max_num_records,
            )?)
        } else {
            None
        };

        Ok((typed_service, inflection_service))
    }

    pub fn new(grpc_config: GrpcApiOptions, security: Option<ApiSecurity>, flags: Flags) -> Self {
        Self {
            port: grpc_config.port.unwrap_or_else(default_grpc_port),
            host: grpc_config.host.unwrap_or_else(default_host),
            security,
            flags,
        }
    }

    /// TcpIncoming::new requires a tokio runtime, so we mark this function as async.
    pub async fn run(
        &self,
        cache_endpoints: Vec<Arc<CacheEndpoint>>,
        shutdown: ShutdownReceiver,
        operations_receiver: Option<Receiver<Operation>>,
        labels: LabelsAndProgress,
        default_max_num_records: usize,
    ) -> Result<
        impl Future<Output = Result<(), dozer_services::tonic::transport::Error>>,
        ApiInitError,
    > {
        let grpc_web = self.flags.grpc_web.unwrap_or(true);
        // Create our services.
        let common_service = CommonGrpcServiceServer::new(CommonService::new(
            cache_endpoints.clone(),
            operations_receiver.as_ref().map(|r| r.resubscribe()),
            default_max_num_records,
        ));
        let common_service = enable_grpc_web(common_service, grpc_web);

        let (typed_service, reflection_service) = self.get_dynamic_service(
            cache_endpoints,
            operations_receiver,
            default_max_num_records,
        )?;
        let typed_service =
            typed_service.map(|typed_service| enable_grpc_web(typed_service, grpc_web));
        let reflection_service = enable_grpc_web(reflection_service, grpc_web);

        let mut service_map: HashMap<String, ServingStatus> = HashMap::new();
        service_map.insert("".to_string(), ServingStatus::Serving);
        service_map.insert(common::SERVICE_NAME.to_string(), ServingStatus::Serving);
        if typed_service.is_some() {
            service_map.insert(typed::SERVICE_NAME.to_string(), ServingStatus::Serving);
        } else {
            service_map.insert(typed::SERVICE_NAME.to_string(), ServingStatus::NotServing);
        }
        let health_service = HealthGrpcServiceServer::new(HealthService {
            serving_status: service_map,
        });
        let health_service = enable_grpc_web(health_service, grpc_web);

        // Auth middleware.
        let security = get_api_security(self.security.to_owned());
        let auth_middleware = AuthMiddlewareLayer::new(security.clone());

        // Authenticated services.
        let common_service = auth_middleware.layer(common_service);
        let typed_service = typed_service.map(|typed_service| auth_middleware.layer(typed_service));
        let mut authenticated_reflection_service = None;
        let mut unauthenticated_reflection_service = None;
        if self.flags.authenticate_server_reflection.unwrap_or(false) {
            authenticated_reflection_service = Some(auth_middleware.layer(reflection_service))
        } else {
            unauthenticated_reflection_service = Some(reflection_service);
        };

        let mut auth_service = None;
        let security = get_api_security(self.security.to_owned());
        if security.is_some() {
            let service = enable_grpc_web(
                AuthGrpcServiceServer::new(AuthService::new(security.to_owned())),
                grpc_web,
            );
            auth_service = Some(auth_middleware.layer(service));
        }
        let metric_middleware = MetricMiddlewareLayer::new(labels);
        // Add services to server.
        let mut grpc_router = Server::builder()
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(trace::DefaultMakeSpan::new().level(Level::INFO))
                    .on_response(trace::DefaultOnResponse::new().level(Level::INFO))
                    .on_failure(trace::DefaultOnFailure::new().level(Level::ERROR)),
            )
            .layer(metric_middleware)
            .accept_http1(true)
            .concurrency_limit_per_connection(32)
            .add_service(common_service)
            .add_optional_service(typed_service);

        if let Some(reflection_service) = authenticated_reflection_service {
            grpc_router = grpc_router.add_service(reflection_service);
        }
        if let Some(reflection_service) = unauthenticated_reflection_service {
            grpc_router = grpc_router.add_service(reflection_service);
        }

        grpc_router = grpc_router.add_service(health_service);
        grpc_router = grpc_router.add_optional_service(auth_service);

        // Start listening.
        let addr = format!("{}:{}", self.host, self.port);
        info!(
            "Starting gRPC server on {addr} with security: {}",
            security.as_ref().map_or("None".to_string(), |s| match s {
                ApiSecurity::Jwt(_) => "JWT".to_string(),
            })
        );
        let addr = addr
            .parse()
            .map_err(|e| GrpcError::AddrParse(addr.clone(), e))?;
        let incoming =
            TcpIncoming::new(addr, true, None).map_err(|e| GrpcError::Listen(addr, e))?;

        // Run server.
        Ok(run_server(grpc_router, incoming, shutdown))
    }
}
