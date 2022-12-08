use super::{
    common::CommonService, common_grpc::common_grpc_service_server::CommonGrpcServiceServer,
    internal_grpc::PipelineRequest, typed::TypedService,
};
use crate::{
    errors::GRPCError, generator::protoc::generator::ProtoGenerator, CacheEndpoint, PipelineDetails,
};
use dozer_cache::cache::Cache;
use dozer_types::{log::info, models::api_config::ApiGrpc, types::Schema};
use futures_util::FutureExt;
use std::{
    collections::HashMap,
    fs,
    path::Path,
    sync::{atomic::AtomicBool, Arc},
    thread,
    time::Duration,
};
use tokio::sync::broadcast;
use tonic::transport::Server;
use tonic_reflection::server::{ServerReflection, ServerReflectionServer};

pub struct ApiServer {
    port: u16,
    dynamic: bool,
    event_notifier: crossbeam::channel::Receiver<PipelineRequest>,
    web: bool,
    url: String,
}

impl ApiServer {
    pub fn new(
        event_notifier: crossbeam::channel::Receiver<PipelineRequest>,
        grpc_config: ApiGrpc,
        dynamic: bool,
    ) -> Self {
        Self {
            port: grpc_config.port as u16,
            web: grpc_config.web,
            url: grpc_config.url,
            event_notifier,
            dynamic,
        }
    }
    pub fn setup_broad_cast_channel(
        sender: broadcast::Sender<PipelineRequest>,
        event_notifier: crossbeam::channel::Receiver<PipelineRequest>,
    ) -> Result<(), GRPCError> {
        let _thread = thread::spawn(move || {
            while let Some(event) = event_notifier.iter().next() {
                _ = sender.send(event);
            }
        });
        Ok(())
    }
    fn get_dynamic_service(
        &self,
        pipeline_map: HashMap<String, PipelineDetails>,
        rx1: broadcast::Receiver<PipelineRequest>,
        _running: Arc<AtomicBool>,
    ) -> Result<(TypedService, ServerReflectionServer<impl ServerReflection>), GRPCError> {
        let mut schema_map: HashMap<String, Schema> = HashMap::new();

        // wait until all schemas are initalized
        for (endpoint_name, details) in &pipeline_map {
            let cache = details.cache_endpoint.cache.clone();
            let mut idx = 0;
            loop {
                let schema_res = cache.get_schema_and_indexes_by_name(endpoint_name);

                match schema_res {
                    Ok((schema, _)) => {
                        schema_map.insert(endpoint_name.clone(), schema);
                        break;
                    }
                    Err(_) => {
                        info!(
                            "Schema for endpoint: {} not found. Waiting...({})",
                            endpoint_name, idx
                        );
                        thread::sleep(Duration::from_millis(300));
                        idx += 1;
                    }
                }
            }
        }
        info!("Schemas initialized. Starting gRPC server.");

        let folder_path = Path::new("./.dozer").join("generated");
        if folder_path.exists() {
            fs::remove_dir_all(&folder_path).unwrap();
        }
        fs::create_dir_all(&folder_path).unwrap();

        let proto_res = ProtoGenerator::generate(
            folder_path.to_string_lossy().to_string(),
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
            rx1.resubscribe(),
        );
        Ok((typed_service, inflection_service))
    }

    pub async fn run(
        &self,
        cache_endpoints: Vec<CacheEndpoint>,
        running: Arc<AtomicBool>,
        receiver_shutdown: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<(), GRPCError> {
        // create broadcast channel
        let (tx, rx1) = broadcast::channel::<PipelineRequest>(16);
        let mut pipeline_map: HashMap<String, PipelineDetails> = HashMap::new();

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
            event_notifier: rx1.resubscribe(),
        });

        let mut grpc_router = Server::builder()
            .accept_http1(true)
            .concurrency_limit_per_connection(32)
            .add_service(
                tonic_web::config()
                    .allow_all_origins()
                    .enable(common_service),
            );

        let grpc_router = if self.dynamic {
            let (grpc_service, inflection_service) =
                self.get_dynamic_service(pipeline_map.clone(), rx1, running)?;
            // GRPC service to handle reflection requests
            grpc_router = grpc_router.add_service(inflection_service);
            if self.web {
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
        ApiServer::setup_broad_cast_channel(tx, self.event_notifier.to_owned())?;
        let addr = format!("{:}:{:}", self.url, self.port).parse().unwrap();
        grpc_router
            .serve_with_shutdown(addr, receiver_shutdown.map(drop))
            .await
            .map_err(|e| GRPCError::InternalError(Box::new(e)))
    }
}
