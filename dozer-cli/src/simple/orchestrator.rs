use super::executor::Executor;
use crate::errors::OrchestrationError;
use crate::pipeline::{LogSinkSettings, PipelineBuilder};
use crate::shutdown::ShutdownReceiver;
use crate::simple::helper::validate_config;
use crate::simple::migration::{create_migration, modify_schema, needs_migration};
use crate::utils::{
    get_api_security_config, get_cache_manager_options, get_executor_options,
    get_file_buffer_capacity, get_grpc_config, get_rest_config,
};

use crate::{flatten_join_handle, join_handle_map_err, Orchestrator};
use dozer_api::auth::{Access, Authorizer};
use dozer_api::{grpc, rest, CacheEndpoint};
use dozer_cache::cache::LmdbRwCacheManager;
use dozer_cache::dozer_log::home_dir::HomeDir;
use dozer_cache::dozer_log::schemas::MigrationSchema;
use dozer_core::app::AppPipeline;
use dozer_core::dag_schemas::DagSchemas;

use crate::console_helper::get_colored_text;
use crate::console_helper::GREEN;
use crate::console_helper::PURPLE;
use crate::console_helper::RED;
use dozer_core::errors::ExecutionError;
use dozer_ingestion::connectors::{SourceSchema, TableInfo};
use dozer_sql::pipeline::builder::statement_to_pipeline;
use dozer_sql::pipeline::errors::PipelineError;
use dozer_types::crossbeam::channel::{self, Sender};
use dozer_types::indicatif::{MultiProgress, ProgressDrawTarget};
use dozer_types::log::info;
use dozer_types::models::app_config::Config;
use dozer_types::tracing::error;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
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
        let progress_draw_target = if atty::is(atty::Stream::Stderr) {
            ProgressDrawTarget::stderr()
        } else {
            ProgressDrawTarget::hidden()
        };
        Self {
            config,
            runtime,
            multi_pb: MultiProgress::with_draw_target(progress_draw_target),
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
                    .map_err(OrchestrationError::CacheInitFailed)?,
            );
            let home_dir = HomeDir::new(
                self.config.home_dir.as_ref(),
                self.config.cache_dir.clone().into(),
            );
            let mut cache_endpoints = vec![];
            for endpoint in &self.config.endpoints {
                let (cache_endpoint, handle) = CacheEndpoint::new(
                    &home_dir,
                    &*cache_manager,
                    endpoint.clone(),
                    Box::pin(shutdown.create_shutdown_future()),
                    operations_sender.clone(),
                    Some(self.multi_pb.clone()),
                )
                .await?;
                let cache_name = endpoint.name.clone();
                futures.push(flatten_join_handle(join_handle_map_err(handle, move |e| {
                    if e.is_map_full() {
                        OrchestrationError::CacheFull(cache_name)
                    } else {
                        OrchestrationError::CacheBuildFailed(e)
                    }
                })));
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
        err_threshold: Option<u32>,
    ) -> Result<(), OrchestrationError> {
        let mut global_err_threshold: Option<u32> = self.config.err_threshold;
        if err_threshold.is_some() {
            global_err_threshold = err_threshold;
        }

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
            file_buffer_capacity: get_file_buffer_capacity(&self.config) as usize,
        };
        let dag_executor = executor.create_dag_executor(
            self.runtime.clone(),
            settings,
            get_executor_options(&self.config, global_err_threshold),
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
        if let Some(api_config) = &self.config.api {
            if let Some(api_security) = &api_config.api_security {
                match api_security {
                    dozer_types::models::api_security::ApiSecurity::Jwt(secret) => {
                        let auth = Authorizer::new(secret, None, None);
                        let token = auth
                            .generate_token(Access::All, None)
                            .map_err(OrchestrationError::GenerateTokenFailed)?;
                        return Ok(token);
                    }
                }
            }
        }
        Err(OrchestrationError::MissingSecurityConfig)
    }

    fn migrate(&mut self, force: bool) -> Result<(), OrchestrationError> {
        let home_dir = HomeDir::new(
            self.config.home_dir.as_ref(),
            self.config.cache_dir.clone().into(),
        );

        info!(
            "Initiating app: {}",
            get_colored_text(&self.config.app_name, PURPLE)
        );
        if force {
            self.clean()?;
        }
        validate_config(&self.config)?;

        // Calculate schemas.
        let endpoint_and_log_paths = self
            .config
            .endpoints
            .iter()
            // We're not really going to run the pipeline, so we put dummy log paths.
            .map(|endpoint| (endpoint.clone(), Default::default()))
            .collect();
        let builder = PipelineBuilder::new(
            &self.config.connections,
            &self.config.sources,
            self.config.sql.as_deref(),
            endpoint_and_log_paths,
            self.multi_pb.clone(),
        );
        let settings = LogSinkSettings {
            file_buffer_capacity: get_file_buffer_capacity(&self.config) as usize,
        };
        let dag = builder.build(self.runtime.clone(), settings)?;
        // Populate schemas.
        let dag_schemas = DagSchemas::new(dag)?;

        // Migrate endpoints one by one.
        let schemas = dag_schemas.get_sink_schemas();
        let enable_token = self
            .config
            .api
            .as_ref()
            .map(|api| api.api_security.is_some())
            .unwrap_or(false);
        let enable_on_event = self
            .config
            .flags
            .as_ref()
            .map(|flags| flags.push_events)
            .unwrap_or(false);
        for (endpoint_name, (schema, connections)) in schemas {
            info!("Migrating endpoint: {endpoint_name}");
            let endpoint = self
                .config
                .endpoints
                .iter()
                .find(|e| e.name == *endpoint_name)
                .expect("Sink name must be the same as endpoint name");
            let (schema, secondary_indexes) = modify_schema(&schema, endpoint)?;
            let schema = MigrationSchema {
                schema,
                secondary_indexes,
                enable_token,
                enable_on_event,
                connections,
            };

            if let Some(migration_id) = needs_migration(&home_dir, &endpoint_name, &schema)? {
                let migration_name = migration_id.name().to_string();
                create_migration(&home_dir, &endpoint_name, migration_id, &schema)?;
                info!("Created new migration {migration_name} for endpoint: {endpoint_name}");
            } else {
                info!("Migration not needed for endpoint: {endpoint_name}");
            }
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

    fn run_all(
        &mut self,
        shutdown: ShutdownReceiver,
        err_threshold: Option<u32>,
    ) -> Result<(), OrchestrationError> {
        let shutdown_api = shutdown.clone();

        let mut dozer_api = self.clone();

        let (tx, rx) = channel::unbounded::<bool>();

        self.migrate(false)?;

        let mut dozer_pipeline = self.clone();
        let pipeline_thread =
            thread::spawn(move || dozer_pipeline.run_apps(shutdown, Some(tx), err_threshold));

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
                get_colored_text("X", RED),
                e
            );
            Err(e)
        },
        |_| {
            info!(
                "[sql][{}]  Transforms validation completed",
                get_colored_text("✓", GREEN)
            );
            Ok(())
        },
    )
}
