use super::executor::{run_dag_executor, Executor};
use super::Contract;
use crate::errors::OrchestrationError;
use crate::pipeline::PipelineBuilder;
use crate::shutdown::ShutdownReceiver;
use crate::simple::build;
use crate::simple::helper::validate_config;
use crate::utils::{
    get_api_security_config, get_app_grpc_config, get_cache_manager_options,
    get_checkpoint_factory_options, get_default_max_num_records, get_executor_options,
    get_grpc_config, get_rest_config,
};

use crate::{flatten_join_handle, join_handle_map_err};
use dozer_api::auth::{Access, Authorizer};
use dozer_api::grpc::internal::internal_pipeline_server::start_internal_pipeline_server;
use dozer_api::{get_api_security, grpc, rest, CacheEndpoint};
use dozer_cache::cache::LmdbRwCacheManager;
use dozer_cache::dozer_log::camino::Utf8PathBuf;
use dozer_cache::dozer_log::home_dir::HomeDir;
use dozer_core::app::AppPipeline;
use dozer_core::dag_schemas::DagSchemas;
use dozer_tracing::LabelsAndProgress;
use dozer_types::constants::LOCK_FILE;
use dozer_types::models::flags::default_push_events;
use tokio::select;

use crate::console_helper::get_colored_text;
use crate::console_helper::GREEN;
use crate::console_helper::PURPLE;
use crate::console_helper::RED;
use dozer_core::errors::ExecutionError;
use dozer_ingestion::connectors::{get_connector, SourceSchema, TableInfo};
use dozer_sql::pipeline::builder::statement_to_pipeline;
use dozer_sql::pipeline::errors::PipelineError;
use dozer_types::log::info;
use dozer_types::models::config::Config;
use dozer_types::tracing::error;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt, TryFutureExt};
use metrics::{describe_counter, describe_histogram};
use std::collections::HashMap;
use std::fs;

use std::sync::mpsc::Sender;
use std::sync::{mpsc, Arc};
use std::thread;
use tokio::runtime::Runtime;
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct SimpleOrchestrator {
    pub base_directory: Utf8PathBuf,
    pub config: Config,
    pub runtime: Arc<Runtime>,
    pub labels: LabelsAndProgress,
}

impl SimpleOrchestrator {
    pub fn new(
        base_directory: Utf8PathBuf,
        config: Config,
        runtime: Arc<Runtime>,
        labels: LabelsAndProgress,
    ) -> Self {
        Self {
            base_directory,
            config,
            runtime,
            labels,
        }
    }

    pub fn run_api(&mut self, shutdown: ShutdownReceiver) -> Result<(), OrchestrationError> {
        describe_histogram!(
            dozer_api::API_LATENCY_HISTOGRAM_NAME,
            "The api processing latency in seconds"
        );
        describe_counter!(
            dozer_api::API_REQUEST_COUNTER_NAME,
            "Number of requests processed by the api"
        );
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

            let internal_grpc_config = get_app_grpc_config(&self.config);
            let app_server_addr = format!(
                "http://{}:{}",
                internal_grpc_config.host, internal_grpc_config.port
            );
            let cache_manager = Arc::new(
                LmdbRwCacheManager::new(get_cache_manager_options(&self.config))
                    .map_err(OrchestrationError::CacheInitFailed)?,
            );
            let default_max_num_records = get_default_max_num_records(&self.config);
            let mut cache_endpoints = vec![];
            for endpoint in &self.config.endpoints {
                let (cache_endpoint, handle) = select! {
                    // If we're shutting down, the cache endpoint will fail to connect
                    _shutdown_future = shutdown.create_shutdown_future() => return Ok(()),
                    result = CacheEndpoint::new(
                        app_server_addr.clone(),
                        &*cache_manager,
                        endpoint.clone(),
                        Box::pin(shutdown.create_shutdown_future()),
                        operations_sender.clone(),
                        self.labels.clone(),

                    ) => result?
                };
                let cache_name = endpoint.name.clone();
                futures.push(flatten_join_handle(join_handle_map_err(handle, move |e| {
                    if e.is_map_full() {
                        OrchestrationError::CacheFull(cache_name)
                    } else {
                        OrchestrationError::CacheBuildFailed(cache_name, e)
                    }
                })));
                cache_endpoints.push(Arc::new(cache_endpoint));
            }

            // Initialize API Server
            let rest_config = get_rest_config(&self.config);
            let rest_handle = if rest_config.enabled {
                let security = get_api_security_config(&self.config).cloned();
                let cache_endpoints_for_rest = cache_endpoints.clone();
                let shutdown_for_rest = shutdown.create_shutdown_future();
                let api_server =
                    rest::ApiServer::new(rest_config, security, default_max_num_records);
                let api_server = api_server
                    .run(
                        cache_endpoints_for_rest,
                        shutdown_for_rest,
                        self.labels.clone(),
                    )
                    .map_err(OrchestrationError::ApiInitFailed)?;
                tokio::spawn(api_server.map_err(OrchestrationError::RestServeFailed))
            } else {
                tokio::spawn(async move { Ok::<(), OrchestrationError>(()) })
            };

            // Initialize gRPC Server
            let grpc_config = get_grpc_config(&self.config);
            let grpc_handle = if grpc_config.enabled {
                let api_security = get_api_security_config(&self.config).cloned();
                let grpc_server = grpc::ApiServer::new(grpc_config, api_security, flags);
                let shutdown = shutdown.create_shutdown_future();
                let grpc_server = grpc_server
                    .run(
                        cache_endpoints,
                        shutdown,
                        operations_receiver,
                        self.labels.clone(),
                        default_max_num_records,
                    )
                    .await
                    .map_err(OrchestrationError::ApiInitFailed)?;
                tokio::spawn(async move {
                    grpc_server
                        .await
                        .map_err(OrchestrationError::GrpcServeFailed)
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

    pub fn home_dir(&self) -> Utf8PathBuf {
        self.base_directory.join(&self.config.home_dir)
    }

    pub fn cache_dir(&self) -> Utf8PathBuf {
        self.base_directory.join(&self.config.cache_dir)
    }

    pub fn lockfile_path(&self) -> Utf8PathBuf {
        self.base_directory.join(LOCK_FILE)
    }

    pub fn run_apps(
        &mut self,
        shutdown: ShutdownReceiver,
        api_notifier: Option<Sender<()>>,
    ) -> Result<(), OrchestrationError> {
        let home_dir = HomeDir::new(self.home_dir(), self.cache_dir());
        let contract = Contract::deserialize(self.lockfile_path().as_std_path())?;
        let executor = self.runtime.block_on(Executor::new(
            &home_dir,
            &contract,
            &self.config.connections,
            &self.config.sources,
            self.config.sql.as_deref(),
            &self.config.endpoints,
            get_checkpoint_factory_options(&self.config),
            self.labels.clone(),
            &self.config.udfs,
        ))?;
        let endpoint_and_logs = executor.endpoint_and_logs().to_vec();
        let dag_executor = self.runtime.block_on(executor.create_dag_executor(
            &self.runtime,
            get_executor_options(&self.config),
            shutdown.clone(),
            self.config.flags.clone().unwrap_or_default(),
        ))?;

        let app_grpc_config = get_app_grpc_config(&self.config);
        let internal_server_future = self
            .runtime
            .block_on(start_internal_pipeline_server(
                endpoint_and_logs,
                &app_grpc_config,
                shutdown.create_shutdown_future(),
            ))
            .map_err(OrchestrationError::InternalServerFailed)?;

        if let Some(api_notifier) = api_notifier {
            api_notifier.send(()).expect("Failed to notify API server");
        }

        let labels = self.labels.clone();
        let pipeline_future = self.runtime.spawn_blocking(move || {
            run_dag_executor(dag_executor, shutdown.get_running_flag(), labels)
        });

        let mut futures = FuturesUnordered::new();
        futures.push(
            internal_server_future
                .map_err(OrchestrationError::GrpcServeFailed)
                .boxed(),
        );
        futures.push(flatten_join_handle(pipeline_future).boxed());

        self.runtime.block_on(async move {
            while let Some(result) = futures.next().await {
                result?;
            }
            Ok(())
        })
    }

    #[allow(clippy::type_complexity)]
    pub fn list_connectors(
        &self,
    ) -> Result<HashMap<String, (Vec<TableInfo>, Vec<SourceSchema>)>, OrchestrationError> {
        self.runtime.block_on(async {
            let mut schema_map = HashMap::new();
            for connection in &self.config.connections {
                let connector = get_connector(connection.clone())?;
                let schema_tuples = connector.list_all_schemas().await?;
                schema_map.insert(connection.name.clone(), schema_tuples);
            }

            Ok(schema_map)
        })
    }

    pub fn generate_token(&self, ttl_in_secs: Option<i32>) -> Result<String, OrchestrationError> {
        if let Some(api_security) = get_api_security(get_api_security_config(&self.config).cloned())
        {
            match api_security {
                dozer_types::models::api_security::ApiSecurity::Jwt(secret) => {
                    let auth = Authorizer::new(&secret, None, None);
                    let duration = ttl_in_secs.map(|f| std::time::Duration::from_secs(f as u64));
                    let token = auth
                        .generate_token(Access::All, duration)
                        .map_err(OrchestrationError::GenerateTokenFailed)?;
                    return Ok(token);
                }
            }
        }
        Err(OrchestrationError::MissingSecurityConfig)
    }

    pub fn build(
        &mut self,
        force: bool,
        shutdown: ShutdownReceiver,
        locked: bool,
    ) -> Result<(), OrchestrationError> {
        let home_dir = self.home_dir();
        let cache_dir = self.cache_dir();
        let home_dir = HomeDir::new(home_dir, cache_dir);

        info!(
            "Initiating app: {}",
            get_colored_text(&self.config.app_name, PURPLE)
        );
        if force {
            self.clean()?;
        }
        validate_config(&self.config)?;

        // Calculate schemas.
        let endpoint_and_logs = self
            .config
            .endpoints
            .iter()
            // We're not really going to run the pipeline, so we don't create logs.
            .map(|endpoint| (endpoint.clone(), None))
            .collect();
        let builder = PipelineBuilder::new(
            &self.config.connections,
            &self.config.sources,
            self.config.sql.as_deref(),
            endpoint_and_logs,
            self.labels.clone(),
            self.config.flags.clone().unwrap_or_default(),
            &self.config.udfs,
        );
        let dag = self
            .runtime
            .block_on(builder.build(&self.runtime, shutdown))?;
        // Populate schemas.
        let dag_schemas = DagSchemas::new(dag)?;

        // Get current contract.
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
            .unwrap_or_else(default_push_events);
        let version = self.config.version as usize;

        let contract = build::Contract::new(
            version,
            &dag_schemas,
            &self.config.connections,
            &self.config.endpoints,
            enable_token,
            enable_on_event,
        )?;

        let contract_path = self.lockfile_path();
        let existing_contract = Contract::deserialize(contract_path.as_std_path()).ok();
        if locked {
            let Some(existing_contract) = existing_contract.as_ref() else {
                return Err(OrchestrationError::LockedNoLockFile);
            };

            if &contract != existing_contract {
                return Err(OrchestrationError::LockedOutdatedLockfile);
            }
        }

        // Run build
        self.runtime.block_on(build::build(
            &home_dir,
            &contract,
            existing_contract.as_ref(),
        ))?;

        contract.serialize(contract_path.as_std_path())?;

        Ok(())
    }

    // Cleaning the entire folder as there will be inconsistencies
    // between pipeline, cache and generated proto files.
    pub fn clean(&mut self) -> Result<(), OrchestrationError> {
        let cache_dir = self.cache_dir();
        if cache_dir.exists() {
            fs::remove_dir_all(&cache_dir)
                .map_err(|e| ExecutionError::FileSystemError(cache_dir.into_std_path_buf(), e))?;
        };

        let home_dir = self.home_dir();
        if home_dir.exists() {
            fs::remove_dir_all(&home_dir)
                .map_err(|e| ExecutionError::FileSystemError(home_dir.into_std_path_buf(), e))?;
        };

        Ok(())
    }

    pub fn run_all(
        &mut self,
        shutdown: ShutdownReceiver,
        locked: bool,
    ) -> Result<(), OrchestrationError> {
        let mut dozer_api = self.clone();

        let (tx, rx) = mpsc::channel::<()>();

        self.build(false, shutdown.clone(), locked)?;

        let mut dozer_pipeline = self.clone();
        let pipeline_shutdown = shutdown.clone();
        let pipeline_thread =
            thread::spawn(move || dozer_pipeline.run_apps(pipeline_shutdown, Some(tx)));

        // Wait for pipeline to initialize caches before starting api server
        if rx.recv().is_err() {
            // This means the pipeline thread returned before sending a message. Either an error happened or it panicked.
            return match pipeline_thread.join() {
                Ok(Err(e)) => Err(e),
                Ok(Ok(())) => panic!("An error must have happened"),
                Err(e) => {
                    std::panic::panic_any(e);
                }
            };
        }

        dozer_api.run_api(shutdown)?;

        // wait for pipeline thread to shutdown gracefully
        pipeline_thread.join().unwrap()
    }
}

pub fn validate_sql(sql: String) -> Result<(), PipelineError> {
    statement_to_pipeline(
        &sql,
        &mut AppPipeline::new_with_default_flags(),
        None,
        vec![],
    )
    .map_or_else(
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
