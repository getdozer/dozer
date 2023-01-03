use super::{
    auth_middleware::AuthMiddlewareLayer,
    common::CommonService,
    common_grpc::common_grpc_service_server::CommonGrpcServiceServer,
    health_grpc::health_grpc_service_server::HealthGrpcServiceServer,
    internal_grpc::{
        internal_pipeline_service_client::InternalPipelineServiceClient, PipelineRequest,
        PipelineResponse,
    },
    typed::TypedService,
};
use crate::grpc::health::HealthService;
use crate::grpc::health_grpc::health_check_response::ServingStatus;
use crate::grpc::{common, typed};
use crate::{
    errors::GRPCError, generator::protoc::generator::ProtoGenerator, CacheEndpoint, PipelineDetails,
};
use dozer_cache::cache::Cache;
use dozer_types::{
    log::info,
    models::{
        api_config::{ApiGrpc, ApiPipelineInternal},
        api_security::ApiSecurity,
        app_config::Flags,
    },
    types::Schema,
};
use futures_util::{FutureExt, StreamExt};
use std::{collections::HashMap, path::PathBuf};
use tokio::sync::broadcast::{self, Receiver, Sender};
use tonic::{transport::Server, Streaming};
use tonic_reflection::server::{ServerReflection, ServerReflectionServer};

pub struct ApiServer {
    port: u16,
    host: String,
    api_dir: PathBuf,
    security: Option<ApiSecurity>,
    flags: Flags,
}

impl ApiServer {
    async fn connect_internal_client(
        pipeline_config: ApiPipelineInternal,
    ) -> Result<Streaming<PipelineResponse>, GRPCError> {
        let address = format!("http://{:}:{:}", pipeline_config.host, pipeline_config.port);
        let mut client = InternalPipelineServiceClient::connect(address)
            .await
            .map_err(|err| GRPCError::InternalError(Box::new(err)))?;

        let stream_response = client
            .stream_pipeline_request(PipelineRequest {})
            .await
            .map_err(|err| GRPCError::InternalError(Box::new(err)))?;
        let stream: Streaming<PipelineResponse> = stream_response.into_inner();
        Ok(stream)
    }
    fn get_dynamic_service(
        &self,
        pipeline_map: HashMap<String, PipelineDetails>,
        rx1: Option<broadcast::Receiver<PipelineResponse>>,
    ) -> Result<(TypedService, ServerReflectionServer<impl ServerReflection>), GRPCError> {
        let mut schema_map: HashMap<String, Schema> = HashMap::new();

        for (endpoint_name, details) in &pipeline_map {
            let cache = details.cache_endpoint.cache.clone();

            let (schema, _) = cache
                .get_schema_and_indexes_by_name(endpoint_name)
                .map_err(|e| GRPCError::SchemaNotInitialized(endpoint_name.clone(), e))?;
            schema_map.insert(endpoint_name.clone(), schema);
        }
        info!(
            "Starting gRPC server on host: {}, port: {}, security: {}",
            self.host,
            self.port,
            self.security
                .as_ref()
                .map_or("None".to_string(), |s| match s {
                    ApiSecurity::Jwt(_) => "JWT".to_string(),
                })
        );

        let generated_path = self.api_dir.join("generated");

        let proto_res = ProtoGenerator::read(
            generated_path.to_string_lossy().to_string(),
            pipeline_map.to_owned(),
        )?;

        let inflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(proto_res.descriptor_bytes.as_slice())
            .build()?;

        // Service handling dynamic gRPC requests.
        let typed_service = TypedService::new(
            proto_res.descriptor,
            pipeline_map,
            schema_map,
            rx1.map(|r| r.resubscribe()),
            self.security.to_owned(),
        );

        Ok((typed_service, inflection_service))
    }

    pub fn new(
        grpc_config: ApiGrpc,
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
        cache_endpoints: Vec<CacheEndpoint>,
        receiver_shutdown: tokio::sync::oneshot::Receiver<()>,
        rx1: Option<Receiver<PipelineResponse>>,
    ) -> Result<(), GRPCError> {
        let mut pipeline_map: HashMap<String, PipelineDetails> = HashMap::new();
        let mut service_map: HashMap<String, ServingStatus> = HashMap::new();
        for ce in cache_endpoints {
            pipeline_map.insert(
                ce.endpoint.name.to_owned(),
                PipelineDetails {
                    schema_name: ce.endpoint.name.to_owned(),
                    cache_endpoint: ce.to_owned(),
                },
            );
        }

        let common_service = CommonGrpcServiceServer::new(CommonService {
            pipeline_map: pipeline_map.to_owned(),
            event_notifier: rx1.as_ref().map(|r| r.resubscribe()),
        });

        // middleware
        let layer = tower::ServiceBuilder::new()
            .layer(AuthMiddlewareLayer::new(self.security.to_owned()))
            .into_inner();

        let mut grpc_router = Server::builder()
            .accept_http1(true)
            .layer(layer)
            .concurrency_limit_per_connection(32)
            .add_service(
                tonic_web::config()
                    .allow_all_origins()
                    .enable(common_service),
            );

        service_map.insert(common::SERVICE_NAME.to_string(), ServingStatus::Serving);

        let mut grpc_router = if self.flags.dynamic {
            let _get_dynamic_service = self.get_dynamic_service(pipeline_map.clone(), rx1);
            if _get_dynamic_service.is_err() {
                service_map.insert(typed::SERVICE_NAME.to_string(), ServingStatus::NotServing);
            } else {
                service_map.insert(typed::SERVICE_NAME.to_string(), ServingStatus::Serving);
            }
            let (grpc_service, inflection_service) = _get_dynamic_service?;
            // GRPC service to handle reflection requests
            grpc_router = grpc_router.add_service(inflection_service);
            if self.flags.grpc_web {
                grpc_router = grpc_router
                    .add_service(tonic_web::config().allow_all_origins().enable(grpc_service));
            } else {
                grpc_router = grpc_router.add_service(grpc_service);
            }
            // GRPC service to handle typed requests
            grpc_router
        } else {
            grpc_router
        };

        let health_service = HealthGrpcServiceServer::new(HealthService {
            serving_status: service_map,
        });

        grpc_router = grpc_router.add_service(
            tonic_web::config()
                .allow_all_origins()
                .enable(health_service),
        );

        let addr = format!("{:}:{:}", self.host, self.port).parse().unwrap();
        grpc_router
            .serve_with_shutdown(addr, receiver_shutdown.map(drop))
            .await
            .map_err(|e| GRPCError::InternalError(Box::new(e)))
    }

    pub fn setup_broad_cast_channel(
        tx: Sender<PipelineResponse>,
        pipeline_config: ApiPipelineInternal,
    ) -> Result<(), GRPCError> {
        tokio::spawn(async move {
            info!(
                "Connecting to Internal service  on http://{}:{}",
                pipeline_config.host, pipeline_config.port
            );
            let mut stream = ApiServer::connect_internal_client(pipeline_config.to_owned())
                .await
                .unwrap();
            while let Some(event_response) = stream.next().await {
                if let Ok(event) = event_response {
                    let _ = tx.send(event);
                }
            }
        });
        Ok(())
    }
}
