use super::executor::Executor;
use crate::console_helper::get_colored_text;
use crate::errors::OrchestrationError;
use crate::pipeline::{CacheSinkSettings, PipelineBuilder};
use crate::simple::helper::validate_config;
use crate::utils::{
    get_api_dir, get_api_security_config, get_app_grpc_config, get_app_max_map_size,
    get_buffer_size, get_cache_dir, get_cache_max_map_size, get_commit_size,
    get_commit_time_threshold, get_flags, get_grpc_config, get_pipeline_dir, get_rest_config,
};
use crate::{flatten_joinhandle, Orchestrator};
use dozer_api::auth::{Access, Authorizer};
use dozer_api::generator::protoc::generator::ProtoGenerator;
use dozer_api::{
    actix_web::dev::ServerHandle,
    grpc::{
        self, internal::internal_pipeline_server::start_internal_pipeline_server,
        internal_grpc::PipelineResponse,
    },
    rest, RoCacheEndpoint,
};
use dozer_cache::cache::{CacheManager, CacheManagerOptions, LmdbCacheManager};
use dozer_core::app::AppPipeline;
use dozer_core::dag_schemas::{DagHaveSchemas, DagSchemas};
use dozer_core::errors::ExecutionError::InternalError;
use dozer_core::executor::ExecutorOptions;
use dozer_core::petgraph::visit::{IntoEdgesDirected, IntoNodeReferences};
use dozer_core::petgraph::Direction;
use dozer_core::NodeKind;
use dozer_sql::pipeline::builder::statement_to_pipeline;
use dozer_sql::pipeline::errors::PipelineError;
use dozer_types::crossbeam::channel::{self, unbounded, Sender};
use dozer_types::log::{info, warn};
use dozer_types::models::app_config::Config;
use dozer_types::tracing::error;
use dozer_types::types::{Operation, Schema, SourceSchema};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{sync::Arc, thread};
use tokio::sync::{broadcast, oneshot};

#[derive(Default, Clone)]
pub struct SimpleOrchestrator {
    pub config: Config,
    pub cache_manager_options: CacheManagerOptions,
    pub executor_options: ExecutorOptions,
}

impl SimpleOrchestrator {
    pub fn new(config: Config) -> Self {
        let cache_manager_options = CacheManagerOptions {
            path: Some(get_cache_dir(&config)),
            max_size: get_cache_max_map_size(&config) as usize,
            ..CacheManagerOptions::default()
        };

        let executor_options = ExecutorOptions {
            commit_sz: get_commit_size(&config),
            channel_buffer_sz: get_buffer_size(&config) as usize,
            commit_time_threshold: get_commit_time_threshold(&config),
            max_map_size: get_app_max_map_size(&config) as usize,
        };
        Self {
            config,
            cache_manager_options,
            executor_options,
        }
    }
}

impl Orchestrator for SimpleOrchestrator {
    fn run_api(&mut self, running: Arc<AtomicBool>) -> Result<(), OrchestrationError> {
        // Channel to communicate CtrlC with API Server
        let (tx, rx) = unbounded::<ServerHandle>();

        // Flags
        let flags = self.config.flags.clone().unwrap_or_default();

        let cache_manager = LmdbCacheManager::new(self.cache_manager_options.clone())
            .map_err(OrchestrationError::CacheInitFailed)?;
        let mut cache_endpoints = vec![];
        for ce in &self.config.endpoints {
            let cache = cache_manager
                .open_ro_cache(&ce.name)
                .map_err(OrchestrationError::CacheInitFailed)?
                .unwrap_or_else(|| {
                    panic!(
                        "Cache for endpoint {} not found. Did you run `dozer app run`?",
                        ce.name
                    )
                });
            cache_endpoints.push(RoCacheEndpoint::new(cache.into(), ce.clone()));
        }

        let ce2 = cache_endpoints.clone();

        let rt = tokio::runtime::Runtime::new().expect("Failed to initialize tokio runtime");
        let (sender_shutdown, receiver_shutdown) = oneshot::channel::<()>();
        rt.block_on(async {
            let mut futures = FuturesUnordered::new();

            // Initialize API Server
            let rest_config = get_rest_config(self.config.to_owned());
            let security = get_api_security_config(self.config.to_owned());
            let rest_handle = tokio::spawn(async move {
                let api_server = rest::ApiServer::new(rest_config, security);
                api_server
                    .run(cache_endpoints, tx)
                    .await
                    .map_err(OrchestrationError::ApiServerFailed)
            });
            // Initiate Push Events
            // create broadcast channel
            let pipeline_config = get_app_grpc_config(self.config.to_owned());

            let rx1 = if flags.push_events {
                let (tx, rx1) = broadcast::channel::<PipelineResponse>(16);

                let handle = tokio::spawn(async move {
                    grpc::ApiServer::setup_broad_cast_channel(tx, pipeline_config)
                        .await
                        .map_err(OrchestrationError::GrpcServerFailed)
                });

                futures.push(flatten_joinhandle(handle));

                Some(rx1)
            } else {
                None
            };

            // Initialize GRPC Server

            let api_dir = get_api_dir(&self.config);
            let grpc_config = get_grpc_config(self.config.to_owned());

            let api_security = get_api_security_config(self.config.to_owned());
            let grpc_server = grpc::ApiServer::new(grpc_config, api_dir, api_security, flags);
            let grpc_handle = tokio::spawn(async move {
                grpc_server
                    .run(ce2, receiver_shutdown, rx1)
                    .await
                    .map_err(OrchestrationError::GrpcServerFailed)
            });

            futures.push(flatten_joinhandle(rest_handle));
            futures.push(flatten_joinhandle(grpc_handle));

            while let Some(result) = futures.next().await {
                result?;
            }
            Ok::<(), OrchestrationError>(())
        })?;

        let server_handle = rx
            .recv()
            .map_err(OrchestrationError::GrpcServerHandleError)?;

        // Waiting for Ctrl+C
        while running.load(Ordering::SeqCst) {}
        sender_shutdown.send(()).unwrap();
        rest::ApiServer::stop(server_handle);

        Ok(())
    }

    fn run_apps(
        &mut self,
        running: Arc<AtomicBool>,
        api_notifier: Option<Sender<bool>>,
    ) -> Result<(), OrchestrationError> {
        let pipeline_home_dir = get_pipeline_dir(&self.config);
        // gRPC notifier channel
        let (sender, receiver) = channel::unbounded::<PipelineResponse>();
        let internal_app_config = self.config.to_owned();
        let _intern_pipeline_thread = thread::spawn(move || {
            if let Err(e) = start_internal_pipeline_server(internal_app_config, receiver) {
                std::panic::panic_any(OrchestrationError::InternalServerFailed(e));
            }
            warn!("Shutting down internal pipeline server");
        });

        let executor = Executor::new(
            self.config.clone(),
            self.config.endpoints.clone(),
            running,
            pipeline_home_dir,
        );
        let flags = get_flags(self.config.clone());
        let api_security = get_api_security_config(self.config.clone());
        let settings = CacheSinkSettings::new(get_api_dir(&self.config), flags, api_security);
        let dag_executor = executor.create_dag_executor(
            Some(sender),
            self.cache_manager_options.clone(),
            settings,
            self.executor_options.clone(),
        )?;

        if let Some(api_notifier) = api_notifier {
            api_notifier
                .send(true)
                .expect("Failed to notify API server");
        }

        executor.run_dag_executor(dag_executor)
    }

    fn list_connectors(&self) -> Result<HashMap<String, Vec<SourceSchema>>, OrchestrationError> {
        Executor::get_tables(&self.config.connections)
    }

    fn generate_token(&self) -> Result<String, OrchestrationError> {
        if let Some(api_config) = self.config.api.to_owned() {
            if let Some(api_security) = api_config.api_security {
                match api_security {
                    dozer_types::models::api_security::ApiSecurity::Jwt(secret) => {
                        let auth = Authorizer::new(&secret, None, None);
                        let token = auth.generate_token(Access::All, None).map_err(|err| {
                            OrchestrationError::GenerateTokenFailed(err.to_string())
                        })?;
                        return Ok(token);
                    }
                }
            }
        }
        Err(OrchestrationError::GenerateTokenFailed(
            "Missing api config or security input".to_owned(),
        ))
    }

    fn query(
        &self,
        sql: String,
        sender: Sender<Operation>,
        running: Arc<AtomicBool>,
    ) -> Result<Schema, OrchestrationError> {
        let pipeline_dir = tempdir::TempDir::new("query4")
            .map_err(|e| OrchestrationError::InternalError(Box::new(e)))?;
        let executor = Executor::new(
            self.config.clone(),
            vec![],
            running,
            pipeline_dir.into_path(),
        );

        let dag = executor.query(sql, sender)?;
        let dag_schemas = DagSchemas::new(&dag)?;

        let sink_index = (|| {
            for (node_index, node) in dag_schemas.graph().node_references() {
                if matches!(node.kind, NodeKind::Sink(_)) {
                    return node_index;
                }
            }
            panic!("Sink is expected");
        })();

        let schema = dag_schemas
            .graph()
            .edges_directed(sink_index, Direction::Incoming)
            .next()
            .expect("Sink must have incoming edge")
            .weight()
            .schema
            .clone();
        Ok(schema)
    }

    fn migrate(&mut self, force: bool) -> Result<(), OrchestrationError> {
        let pipeline_home_dir = get_pipeline_dir(&self.config);
        let api_dir = get_api_dir(&self.config);
        let cache_dir = get_cache_dir(&self.config);

        info!(
            "Initiating app: {}",
            get_colored_text(&self.config.app_name, "35")
        );
        if api_dir.exists() || pipeline_home_dir.exists() || cache_dir.exists() {
            if force {
                self.clean()?;
            } else {
                return Err(OrchestrationError::InitializationFailed(
                    self.config.home_dir.to_string(),
                ));
            }
        }
        validate_config(&self.config)?;

        let builder = PipelineBuilder::new(
            self.config.clone(),
            self.config.endpoints.clone(),
            pipeline_home_dir.clone(),
        );

        // Api Path
        if !api_dir.exists() {
            fs::create_dir_all(api_dir.clone()).map_err(|e| InternalError(Box::new(e)))?;
        }

        // Pipeline path
        fs::create_dir_all(pipeline_home_dir.clone()).map_err(|e| {
            OrchestrationError::PipelineDirectoryInitFailed(
                pipeline_home_dir.to_string_lossy().to_string(),
                e,
            )
        })?;
        let api_security = get_api_security_config(self.config.clone());
        let flags = get_flags(self.config.clone());
        let settings = CacheSinkSettings::new(api_dir.clone(), flags, api_security);
        let dag = builder.build(None, self.cache_manager_options.clone(), settings)?;
        // Populate schemas.
        DagSchemas::new(&dag)?;

        let mut resources = Vec::new();
        for e in &self.config.endpoints {
            resources.push(&e.name);
        }

        let common_resources = ProtoGenerator::copy_common(&api_dir)
            .map_err(|e| OrchestrationError::InternalError(Box::new(e)))?;

        // Copy common service to be included in descriptor.
        resources.extend(common_resources.iter());

        // Generate a descriptor based on all proto files generated within sink.
        let descriptor_path = ProtoGenerator::descriptor_path(&api_dir);
        ProtoGenerator::generate_descriptor(&api_dir, &descriptor_path, &resources)
            .map_err(|e| OrchestrationError::InternalError(Box::new(e)))?;

        Ok(())
    }

    // Cleaning the entire folder as there will be inconsistencies
    // between pipeline, cache and generated proto files.
    fn clean(&mut self) -> Result<(), OrchestrationError> {
        let home_dir = PathBuf::from(self.config.home_dir.clone());
        if home_dir.exists() {
            fs::remove_dir_all(&home_dir).map_err(|e| InternalError(Box::new(e)))?;
        };
        Ok(())
    }

    fn run_all(&mut self, running: Arc<AtomicBool>) -> Result<(), OrchestrationError> {
        let running_api = running.clone();
        // TODO: remove this after checkpointing
        self.clean()?;

        let mut dozer_api = self.clone();

        let (tx, rx) = channel::unbounded::<bool>();

        if let Err(e) = self.migrate(false) {
            if let OrchestrationError::InitializationFailed(_) = e {
                warn!(
                    "{} is already present. Skipping initialisation..",
                    self.config.home_dir.to_owned()
                )
            } else {
                return Err(e);
            }
        }

        let mut dozer_pipeline = self.clone();
        let pipeline_thread = thread::spawn(move || {
            if let Err(e) = dozer_pipeline.run_apps(running, Some(tx)) {
                std::panic::panic_any(e);
            }
        });

        // Wait for pipeline to initialize caches before starting api server
        rx.recv().unwrap();

        thread::spawn(move || {
            if let Err(e) = dozer_api.run_api(running_api) {
                std::panic::panic_any(e);
            }
        });

        pipeline_thread.join().unwrap();
        Ok(())
    }
}

pub fn validate_sql(sql: String) -> Result<(), PipelineError> {
    statement_to_pipeline(&sql, &mut AppPipeline::new(), None).map_or_else(
        |e| {
            error!(
                "[sql][{}] Transforms validation error: {}",
                get_colored_text("X", "31"),
                e
            );
            Err(e)
        },
        |_| {
            info!(
                "[sql][{}]  Transforms validation completed",
                get_colored_text("✓", "32")
            );
            Ok(())
        },
    )
}
