use super::{auth_middleware::AuthMiddlewareLayer, common::CommonService, typed::TypedService};
use crate::grpc::health::HealthService;
use crate::grpc::{common, typed};
use crate::{errors::GrpcError, generator::protoc::generator::ProtoGenerator, RoCacheEndpoint};
use dozer_types::grpc_types::health::health_check_response::ServingStatus;
use dozer_types::grpc_types::types::Operation;
use dozer_types::grpc_types::{
    common::common_grpc_service_server::CommonGrpcServiceServer,
    health::health_grpc_service_server::HealthGrpcServiceServer,
};
use dozer_types::tracing::Level;
use dozer_types::{
    log::info,
    models::{api_config::GrpcApiOptions, api_security::ApiSecurity, flags::Flags},
};
use futures_util::FutureExt;
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::sync::broadcast::{self, Receiver};
use tonic::transport::Server;
use tonic_reflection::server::{ServerReflection, ServerReflectionServer};
use tower::Layer;
use tower_http::trace::{self, TraceLayer};

pub struct ApiServer {
    port: u16,
    host: String,
    api_dir: PathBuf,
    security: Option<ApiSecurity>,
    flags: Flags,
}

impl ApiServer {
    fn get_dynamic_service(
        &self,
        cache_endpoints: Vec<Arc<RoCacheEndpoint>>,
        operations_receiver: Option<broadcast::Receiver<Operation>>,
    ) -> Result<
        (
            Option<TypedService>,
            ServerReflectionServer<impl ServerReflection>,
        ),
        GrpcError,
    > {
        info!(
            "Starting gRPC server on http://{}:{} with security: {}",
            self.host,
            self.port,
            self.security
                .as_ref()
                .map_or("None".to_string(), |s| match s {
                    ApiSecurity::Jwt(_) => "JWT".to_string(),
                })
        );

        let descriptor_path = ProtoGenerator::descriptor_path(&self.api_dir);

        let descriptor_bytes = ProtoGenerator::read_descriptor_bytes(&descriptor_path)?;

        let inflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(&descriptor_bytes)
            .build()?;

        // Service handling dynamic gRPC requests.
        let typed_service = if self.flags.dynamic {
            Some(TypedService::new(
                &descriptor_path,
                cache_endpoints,
                operations_receiver,
                self.security.clone(),
            )?)
        } else {
            None
        };

        Ok((typed_service, inflection_service))
    }

    pub fn new(
        grpc_config: GrpcApiOptions,
        api_dir: PathBuf,
        security: Option<ApiSecurity>,
        flags: Flags,
    ) -> Self {
        Self {
            port: grpc_config.port as u16,
            host: grpc_config.host,
            api_dir,
            security,
            flags,
        }
    }

    pub async fn run(
        &self,
        cache_endpoints: Vec<Arc<RoCacheEndpoint>>,
        receiver_shutdown: tokio::sync::oneshot::Receiver<()>,
        operations_receiver: Option<Receiver<Operation>>,
    ) -> Result<(), GrpcError> {
        // Create our services.
        let mut web_config = tonic_web::config();
        if self.flags.grpc_web {
            web_config = web_config.allow_all_origins();
        }

        let common_service = CommonGrpcServiceServer::new(CommonService::new(
            cache_endpoints.clone(),
            operations_receiver.as_ref().map(|r| r.resubscribe()),
        ));
        let common_service = web_config.enable(common_service);

        let (typed_service, reflection_service) =
            self.get_dynamic_service(cache_endpoints, operations_receiver)?;
        let typed_service = typed_service.map(|typed_service| web_config.enable(typed_service));
        let reflection_service = web_config.enable(reflection_service);

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
        let health_service = web_config.enable(health_service);

        // Auth middleware.
        let auth_middleware = AuthMiddlewareLayer::new(self.security.clone());

        // Authenticated services.
        let common_service = auth_middleware.layer(common_service);
        let typed_service = typed_service.map(|typed_service| auth_middleware.layer(typed_service));
        let mut authenticated_reflection_service = None;
        let mut unauthenticated_reflection_service = None;
        if self.flags.authenticate_server_reflection {
            authenticated_reflection_service = Some(auth_middleware.layer(reflection_service))
        } else {
            unauthenticated_reflection_service = Some(reflection_service);
        };
        let health_service = auth_middleware.layer(health_service);

        // Add services to server.
        let mut grpc_router = Server::builder()
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(trace::DefaultMakeSpan::new().level(Level::INFO))
                    .on_response(trace::DefaultOnResponse::new().level(Level::INFO))
                    .on_failure(trace::DefaultOnFailure::new().level(Level::ERROR)),
            )
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

        // Run server.
        let addr = format!("{:}:{:}", self.host, self.port).parse().unwrap();
        grpc_router
            .serve_with_shutdown(addr, receiver_shutdown.map(drop))
            .await?;
        Ok(())
    }
}
