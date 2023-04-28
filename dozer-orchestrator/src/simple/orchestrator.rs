use super::executor::Executor;
use crate::console_helper::get_colored_text;
use crate::errors::OrchestrationError;
use crate::pipeline::{LogSinkSettings, PipelineBuilder};
use crate::shutdown::ShutdownReceiver;
use crate::simple::helper::validate_config;
use crate::utils::{
    get_api_security_config, get_cache_manager_options, get_executor_options,
    get_file_buffer_capacity, get_grpc_config, get_rest_config,
};

use crate::{flatten_join_handle, Orchestrator};
use dozer_api::auth::{Access, Authorizer};
use dozer_api::generator::protoc::generator::ProtoGenerator;
use dozer_api::{grpc, rest, CacheEndpoint};
use dozer_cache::cache::LmdbRwCacheManager;
use dozer_cache::dozer_log::home_dir::HomeDir;
use dozer_cache::dozer_log::schemas::write_schema;
use dozer_core::app::AppPipeline;
use dozer_core::dag_schemas::DagSchemas;

use dozer_api::grpc::internal::internal_pipeline_server::start_internal_pipeline_server;
use dozer_core::errors::ExecutionError;
use dozer_ingestion::connectors::{SourceSchema, TableInfo};
use dozer_sql::pipeline::builder::statement_to_pipeline;
use dozer_sql::pipeline::errors::PipelineError;
use dozer_types::crossbeam::channel::{self, Sender};
use dozer_types::indicatif::MultiProgress;
use dozer_types::log::{info, warn};
use dozer_types::models::app_config::Config;
use dozer_types::tracing::error;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use tokio::runtime::Runtime;
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct SimpleOrchestrator {
    pub config: Config,
    pub runtime: Arc<Runtime>,
    pub multi_pb: MultiProgress,
}

impl SimpleOrchestrator {
    pub fn new(config: Config, runtime: Arc<Runtime>) -> Self {
        Self {
            config,
            runtime,
            multi_pb: MultiProgress::new(),
        }
    }
}

impl Orchestrator for SimpleOrchestrator {
    fn run_api(&mut self, shutdown: ShutdownReceiver) -> Result<(), OrchestrationError> {
        self.runtime.block_on(async {
            let mut futures = FuturesUnordered::new();

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
            let home_dir = HomeDir::new(
                self.config.home_dir.as_ref(),
                self.config.cache_dir.clone().into(),
            );
            let mut cache_endpoints = vec![];
            for endpoint in &self.config.endpoints {
                let (cache_endpoint, task) = CacheEndpoint::new(
                    &home_dir,
                    &*cache_manager,
                    endpoint.clone(),
                    self.runtime.clone(),
                    Box::pin(shutdown.create_shutdown_future()),
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
            let rest_handle = if rest_config.enabled {
                let security = get_api_security_config(self.config.to_owned());
                let cache_endpoints_for_rest = cache_endpoints.clone();
                let shutdown_for_rest = shutdown.create_shutdown_future();
                tokio::spawn(async move {
                    let api_server = rest::ApiServer::new(rest_config, security);
                    api_server
                        .run(cache_endpoints_for_rest, shutdown_for_rest)
                        .await
                        .map_err(OrchestrationError::ApiServerFailed)
                })
            } else {
                tokio::spawn(async move { Ok::<(), OrchestrationError>(()) })
            };

            // Initialize gRPC Server
            let grpc_config = get_grpc_config(self.config.to_owned());
            let grpc_handle = if grpc_config.enabled {
                let api_security = get_api_security_config(self.config.to_owned());
                let grpc_server = grpc::ApiServer::new(grpc_config, api_security, flags);
                let shutdown = shutdown.create_shutdown_future();
                tokio::spawn(async move {
                    grpc_server
                        .run(cache_endpoints, shutdown, operations_receiver)
                        .await
                        .map_err(OrchestrationError::GrpcServerFailed)
                })
            } else {
                tokio::spawn(async move { Ok::<(), OrchestrationError>(()) })
            };

            futures.push(flatten_join_handle(rest_handle));
            futures.push(flatten_join_handle(grpc_handle));

            while let Some(result) = futures.next().await {
                result?;
            }

            Ok::<(), OrchestrationError>(())
        })?;

        Ok(())
    }

    fn run_apps(
        &mut self,
        shutdown: ShutdownReceiver,
        api_notifier: Option<Sender<bool>>,
    ) -> Result<(), OrchestrationError> {
        // gRPC notifier channel
        let (alias_redirected_sender, alias_redirected_receiver) = channel::unbounded();
        let (operation_sender, operation_receiver) = channel::unbounded();
        let (status_update_sender, status_update_receiver) = channel::unbounded();
        let internal_app_config = self.config.clone();
        let _intern_pipeline_thread = self.runtime.spawn(async move {
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

        let home_dir = HomeDir::new(
            self.config.home_dir.as_ref(),
            self.config.cache_dir.clone().into(),
        );
        let executor = Executor::new(
            &home_dir,
            &self.config.connections,
            &self.config.sources,
            self.config.sql.as_deref(),
            &self.config.endpoints,
            shutdown.get_running_flag(),
            self.multi_pb.clone(),
        )?;
        let settings = LogSinkSettings {
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
        let home_dir = HomeDir::new(
            self.config.home_dir.as_ref(),
            self.config.cache_dir.clone().into(),
        );

        info!(
            "Initiating app: {}",
            get_colored_text(&self.config.app_name, "35")
        );
        if force {
            self.clean()?;
        }
        validate_config(&self.config)?;

        // Always create new migration for now.
        let mut endpoint_and_migration_paths = vec![];
        for endpoint in &self.config.endpoints {
            let migration_path =
                home_dir
                    .create_new_migration(&endpoint.name)
                    .map_err(|(path, error)| {
                        OrchestrationError::FailedToCreateMigration(path, error)
                    })?;
            endpoint_and_migration_paths.push((endpoint, migration_path));
        }

        // Calculate schemas.
        let endpoint_and_log_paths = endpoint_and_migration_paths
            .iter()
            .map(|(endpoint, migration_path)| {
                ((*endpoint).clone(), migration_path.log_path.clone())
            })
            .collect();
        let builder = PipelineBuilder::new(
            &self.config.connections,
            &self.config.sources,
            self.config.sql.as_deref(),
            endpoint_and_log_paths,
            self.multi_pb.clone(),
        );
        let settings = LogSinkSettings {
            file_buffer_capacity: get_file_buffer_capacity(&self.config),
        };
        let dag = builder.build(self.runtime.clone(), settings, None)?;
        // Populate schemas.
        let dag_schemas = DagSchemas::new(dag)?;

        // Write schemas to pipeline_dir and generate proto files.
        let schemas = dag_schemas.get_sink_schemas();
        let api_config = self.config.api.clone().unwrap_or_default();
        for (endpoint_name, schema) in &schemas {
            let (endpoint, migration_path) = endpoint_and_migration_paths
                .iter()
                .find(|e| e.0.name == *endpoint_name)
                .expect("Sink name must be the same as endpoint name");

            let schema = write_schema(schema, endpoint, &migration_path.schema_path)
                .map_err(OrchestrationError::FailedToWriteSchema)?;

            let proto_folder_path = &migration_path.api_dir;
            ProtoGenerator::generate(
                proto_folder_path,
                endpoint_name,
                &schema,
                &api_config.api_security,
                &self.config.flags,
            )?;

            let mut resources = Vec::new();
            resources.push(endpoint_name);

            let common_resources = ProtoGenerator::copy_common(proto_folder_path)
                .map_err(|e| OrchestrationError::InternalError(Box::new(e)))?;

            // Copy common service to be included in descriptor.
            resources.extend(common_resources.iter());

            // Generate a descriptor based on all proto files generated within sink.
            ProtoGenerator::generate_descriptor(
                proto_folder_path,
                &migration_path.descriptor_path,
                &resources,
            )
            .map_err(|e| OrchestrationError::InternalError(Box::new(e)))?;
        }

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

    fn run_all(&mut self, shutdown: ShutdownReceiver) -> Result<(), OrchestrationError> {
        let shutdown_api = shutdown.clone();

        let mut dozer_api = self.clone();

        let (tx, rx) = channel::unbounded::<bool>();

        self.migrate(false)?;

        let mut dozer_pipeline = self.clone();
        let pipeline_thread = thread::spawn(move || dozer_pipeline.run_apps(shutdown, Some(tx)));

        // Wait for pipeline to initialize caches before starting api server
        rx.recv().unwrap();

        dozer_api.run_api(shutdown_api)?;

        // wait for pipeline thread to shutdown gracefully
        pipeline_thread.join().unwrap()
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
