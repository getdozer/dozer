use super::executor::Executor;
use super::schemas::load_schemas;
use crate::console_helper::get_colored_text;
use crate::errors::OrchestrationError;
use crate::pipeline::{LogSinkSettings, PipelineBuilder};
use crate::simple::helper::validate_config;
use crate::simple::schemas::write_schemas;
use crate::utils::{
    get_api_dir, get_api_security_config, get_cache_dir, get_cache_manager_options,
    get_endpoint_log_path, get_executor_options, get_file_buffer_capacity, get_grpc_config,
    get_pipeline_dir, get_rest_config,
};
use crate::{flatten_join_handle, Orchestrator};
use dozer_api::auth::{Access, Authorizer};
use dozer_api::generator::protoc::generator::ProtoGenerator;
use dozer_api::{actix_web::dev::ServerHandle, grpc, rest, CacheEndpoint};
use dozer_cache::cache::LmdbRwCacheManager;
use dozer_core::app::AppPipeline;
use dozer_core::dag_schemas::DagSchemas;

use dozer_core::errors::ExecutionError;
use dozer_ingestion::connectors::{SourceSchema, TableInfo};
use dozer_sql::pipeline::builder::statement_to_pipeline;
use dozer_sql::pipeline::errors::PipelineError;
use dozer_types::crossbeam::channel::{self, unbounded, Sender};
use dozer_types::indicatif::MultiProgress;
use dozer_types::log::{info, warn};
use dozer_types::models::app_config::Config;
use dozer_types::tracing::error;

use dozer_api::grpc::internal::internal_pipeline_server::start_internal_pipeline_server;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::{sync::Arc, thread};
use tokio::runtime::Runtime;
use tokio::sync::broadcast;
use tokio::sync::oneshot;

#[derive(Clone)]
pub struct SimpleOrchestrator {
    pub config: Config,
    runtime: Arc<Runtime>,
    pub multi_pb: MultiProgress,
}

impl SimpleOrchestrator {
    pub fn new(config: Config) -> Self {
        let runtime = Arc::new(Runtime::new().expect("Failed to initialize tokio runtime"));
        Self {
            config,
            runtime,
            multi_pb: MultiProgress::new(),
        }
    }
}

impl Orchestrator for SimpleOrchestrator {
    fn run_api(&mut self, _running: Arc<AtomicBool>) -> Result<(), OrchestrationError> {
        // Channel to communicate CtrlC with API Server
        let (tx, rx) = unbounded::<ServerHandle>();

        let (sender_shutdown, receiver_shutdown) = oneshot::channel::<()>();
        self.runtime.block_on(async {
            let mut futures = FuturesUnordered::new();

            // Load schemas.
            let pipeline_path = get_pipeline_dir(&self.config);
            let schemas = load_schemas(&pipeline_path)?;

            // Open `RoCacheEndpoint`s. Streaming operations if necessary.
            let flags = self.config.flags.clone().unwrap_or_default();
            let (operations_sender, operations_receiver) = if flags.dynamic {
                let (sender, receiver) = broadcast::channel(16);
                (Some(sender), Some(receiver))
            } else {
                (None, None)
            };

            let cache_manager = Arc::new(
                LmdbRwCacheManager::new(get_cache_manager_options(&self.config))
                    .map_err(OrchestrationError::RoCacheInitFailed)?,
            );
            let pipeline_dir = get_pipeline_dir(&self.config);
            let mut cache_endpoints = vec![];
            for endpoint in &self.config.endpoints {
                let schema = schemas
                    .get(&endpoint.name)
                    .expect("schema is expected to exist");
                let log_path = get_endpoint_log_path(&pipeline_dir, &endpoint.name);
                let (cache_endpoint, task) = CacheEndpoint::new(
                    &*cache_manager,
                    schema.clone(),
                    endpoint.clone(),
                    self.runtime.clone(),
                    &log_path,
                    operations_sender.clone(),
                    Some(self.multi_pb.clone()),
                )
                .await?;
                if let Some(task) = task {
                    futures.push(flatten_join_handle(tokio::task::spawn_blocking(
                        move || task().map_err(OrchestrationError::CacheBuildFailed),
                    )));
                }
                cache_endpoints.push(Arc::new(cache_endpoint));
            }

            // Initialize API Server
            let rest_config = get_rest_config(self.config.to_owned());
            let security = get_api_security_config(self.config.to_owned());
            let cache_endpoints_for_rest = cache_endpoints.clone();
            let rest_handle = tokio::spawn(async move {
                let api_server = rest::ApiServer::new(rest_config, security);
                api_server
                    .run(cache_endpoints_for_rest, tx)
                    .await
                    .map_err(OrchestrationError::ApiServerFailed)
            });

            // Initialize gRPC Server
            let api_dir = get_api_dir(&self.config);
            let grpc_config = get_grpc_config(self.config.to_owned());
            let api_security = get_api_security_config(self.config.to_owned());
            let grpc_server = grpc::ApiServer::new(grpc_config, api_dir, api_security, flags);
            let grpc_handle = tokio::spawn(async move {
                grpc_server
                    .run(cache_endpoints, receiver_shutdown, operations_receiver)
                    .await
                    .map_err(OrchestrationError::GrpcServerFailed)
            });

            futures.push(flatten_join_handle(rest_handle));
            futures.push(flatten_join_handle(grpc_handle));

            while let Some(result) = futures.next().await {
                result?;
            }
            let server_handle = rx
                .recv()
                .map_err(OrchestrationError::GrpcServerHandleError)?;

            sender_shutdown.send(()).unwrap();
            rest::ApiServer::stop(server_handle);

            Ok::<(), OrchestrationError>(())
        })?;

        Ok(())
    }

    fn run_apps(
        &mut self,
        running: Arc<AtomicBool>,
        api_notifier: Option<Sender<bool>>,
    ) -> Result<(), OrchestrationError> {
        let runtime = Runtime::new().expect("Failed to create runtime for running apps");

        // gRPC notifier channel
        let (alias_redirected_sender, alias_redirected_receiver) = channel::unbounded();
        let (operation_sender, operation_receiver) = channel::unbounded();
        let (status_update_sender, status_update_receiver) = channel::unbounded();
        let internal_app_config = self.config.clone();
        let _intern_pipeline_thread = runtime.spawn(async move {
            let result = start_internal_pipeline_server(
                internal_app_config,
                (
                    alias_redirected_receiver,
                    operation_receiver,
                    status_update_receiver,
                ),
            )
            .await;

            if let Err(e) = result {
                std::panic::panic_any(OrchestrationError::InternalServerFailed(e));
            }

            warn!("Shutting down internal pipeline server");
        });

        let pipeline_dir = get_pipeline_dir(&self.config);
        let executor = Executor::new(
            &self.config.connections,
            &self.config.sources,
            self.config.sql.as_deref(),
            &self.config.endpoints,
            &pipeline_dir,
            running,
            self.multi_pb.clone(),
        );
        let settings = LogSinkSettings {
            pipeline_dir: pipeline_dir.clone(),
            file_buffer_capacity: get_file_buffer_capacity(&self.config),
        };
        let dag_executor = executor.create_dag_executor(
            self.runtime.clone(),
            settings,
            get_executor_options(&self.config),
            Some((
                alias_redirected_sender,
                operation_sender,
                status_update_sender,
            )),
        )?;

        if let Some(api_notifier) = api_notifier {
            api_notifier
                .send(true)
                .expect("Failed to notify API server");
        }

        executor.run_dag_executor(dag_executor)
    }

    fn list_connectors(
        &self,
    ) -> Result<HashMap<String, (Vec<TableInfo>, Vec<SourceSchema>)>, OrchestrationError> {
        self.runtime
            .block_on(Executor::get_tables(&self.config.connections))
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
            &self.config.connections,
            &self.config.sources,
            self.config.sql.as_deref(),
            &self.config.endpoints,
            &pipeline_home_dir,
            self.multi_pb.clone(),
        );

        // Api Path
        if !api_dir.exists() {
            fs::create_dir_all(&api_dir)
                .map_err(|e| ExecutionError::FileSystemError(api_dir, e))?;
        }

        // cache Path
        if !cache_dir.exists() {
            fs::create_dir_all(&cache_dir)
                .map_err(|e| ExecutionError::FileSystemError(cache_dir, e))?;
        }

        // Pipeline path
        fs::create_dir_all(pipeline_home_dir.clone()).map_err(|e| {
            OrchestrationError::PipelineDirectoryInitFailed(
                pipeline_home_dir.to_string_lossy().to_string(),
                e,
            )
        })?;

        let settings = LogSinkSettings {
            pipeline_dir: pipeline_home_dir.clone(),
            file_buffer_capacity: get_file_buffer_capacity(&self.config),
        };
        let dag = builder.build(self.runtime.clone(), settings, None)?;
        // Populate schemas.
        let dag_schemas = DagSchemas::new(dag)?;

        // Write schemas to pipeline_dir and generate proto files.
        let schemas = write_schemas(
            &dag_schemas,
            pipeline_home_dir.clone(),
            &self.config.endpoints,
        )?;
        let api_dir = get_api_dir(&self.config);
        let api_config = self.config.api.clone().unwrap_or_default();
        for (schema_name, schema) in &schemas {
            ProtoGenerator::generate(
                &api_dir,
                schema_name,
                schema,
                &api_config.api_security,
                &self.config.flags,
            )?;
        }

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
        let cache_dir = PathBuf::from(self.config.cache_dir.clone());
        if cache_dir.exists() {
            fs::remove_dir_all(&cache_dir)
                .map_err(|e| ExecutionError::FileSystemError(cache_dir, e))?;
        };

        let home_dir = PathBuf::from(self.config.home_dir.clone());
        if home_dir.exists() {
            fs::remove_dir_all(&home_dir)
                .map_err(|e| ExecutionError::FileSystemError(home_dir, e))?;
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

        let _api_thread = thread::spawn(move || {
            if let Err(e) = dozer_api.run_api(running_api) {
                std::panic::panic_any(e);
            }
        });

        // wait for threads to shutdown gracefully
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
