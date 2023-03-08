use super::executor::Executor;
use crate::console_helper::get_colored_text;
use crate::errors::OrchestrationError;
use crate::pipeline::{CacheSinkSettings, PipelineBuilder};
use crate::simple::helper::validate_config;
use crate::utils::{
    get_api_dir, get_api_security_config, get_app_grpc_config, get_cache_dir,
    get_cache_manager_options, get_executor_options, get_flags, get_grpc_config, get_pipeline_dir,
    get_rest_config,
};
use crate::{flatten_join_handle, Orchestrator};
use dozer_api::auth::{Access, Authorizer};
use dozer_api::generator::protoc::generator::ProtoGenerator;
use dozer_api::grpc::internal::internal_pipeline_client::InternalPipelineClient;
use dozer_api::{
    actix_web::dev::ServerHandle,
    grpc::{self, internal::internal_pipeline_server::start_internal_pipeline_server},
    rest, RoCacheEndpoint,
};
use dozer_cache::cache::{CacheManager, LmdbCacheManager};
use dozer_core::app::AppPipeline;
use dozer_core::dag_schemas::{DagHaveSchemas, DagSchemas};
use dozer_core::errors::ExecutionError::InternalError;
use dozer_core::petgraph::visit::{IntoEdgesDirected, IntoNodeReferences};
use dozer_core::petgraph::Direction;
use dozer_core::NodeKind;
use dozer_sql::pipeline::builder::statement_to_pipeline;
use dozer_sql::pipeline::errors::PipelineError;
use dozer_types::crossbeam::channel::{self, unbounded, Sender};
use dozer_types::grpc_types::internal::AliasRedirected;
use dozer_types::log::{info, warn};
use dozer_types::models::app_config::Config;
use dozer_types::tracing::error;
use dozer_types::types::{Operation, Schema, SourceSchema};
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryFutureExt};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{sync::Arc, thread};
use tokio::sync::broadcast::Receiver;
use tokio::sync::oneshot;

#[derive(Default, Clone)]
pub struct SimpleOrchestrator {
    pub config: Config,
}

impl SimpleOrchestrator {
    pub fn new(config: Config) -> Self {
        Self { config }
    }
}

impl Orchestrator for SimpleOrchestrator {
    fn run_api(&mut self, running: Arc<AtomicBool>) -> Result<(), OrchestrationError> {
        // Channel to communicate CtrlC with API Server
        let (tx, rx) = unbounded::<ServerHandle>();

        let rt = tokio::runtime::Runtime::new().expect("Failed to initialize tokio runtime");
        let (sender_shutdown, receiver_shutdown) = oneshot::channel::<()>();
        rt.block_on(async {
            let mut futures = FuturesUnordered::new();

            // Initiate `AliasRedirected` events, must be done before `RoCacheEndpoint::new` to avoid following scenario:
            // 1. `RoCacheEndpoint::new` is called.
            // 2. App server sends an `AliasRedirected` event.
            // 3. Push event is initiated.
            // In this scenario, the `AliasRedirected` event will be lost and the API server will be serving the wrong cache.
            let app_grpc_config = get_app_grpc_config(self.config.clone());
            let mut internal_pipeline_client =
                InternalPipelineClient::new(&app_grpc_config).await?;
            let (alias_redirected_receiver, future) =
                internal_pipeline_client.stream_alias_events().await?;
            futures.push(flatten_join_handle(tokio::spawn(
                future.map_err(OrchestrationError::GrpcServerFailed),
            )));

            // Open `RoCacheEndpoint`s.
            let cache_manager = LmdbCacheManager::new(get_cache_manager_options(&self.config))
                .map_err(OrchestrationError::CacheInitFailed)?;
            let cache_endpoints = self
                .config
                .endpoints
                .iter()
                .map(|endpoint| {
                    RoCacheEndpoint::new(&cache_manager, endpoint.clone()).map(Arc::new)
                })
                .collect::<Result<Vec<_>, _>>()?;

            // Listen to endpoint redirect events.
            tokio::spawn(redirect_cache_endpoints(
                Box::new(cache_manager),
                cache_endpoints.clone(),
                alias_redirected_receiver,
            ));

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

            // Initialize `PipelineResponse` events.
            let flags = self.config.flags.clone().unwrap_or_default();
            let operation_receiver = if flags.dynamic {
                let (operation_receiver, future) =
                    internal_pipeline_client.stream_operations().await?;
                futures.push(flatten_join_handle(tokio::spawn(
                    future.map_err(OrchestrationError::GrpcServerFailed),
                )));
                Some(operation_receiver)
            } else {
                None
            };

            // Initialize gRPC Server
            let api_dir = get_api_dir(&self.config);
            let grpc_config = get_grpc_config(self.config.to_owned());
            let api_security = get_api_security_config(self.config.to_owned());
            let grpc_server = grpc::ApiServer::new(grpc_config, api_dir, api_security, flags);
            let grpc_handle = tokio::spawn(async move {
                grpc_server
                    .run(cache_endpoints, receiver_shutdown, operation_receiver)
                    .await
                    .map_err(OrchestrationError::GrpcServerFailed)
            });

            futures.push(flatten_join_handle(rest_handle));
            futures.push(flatten_join_handle(grpc_handle));

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
        // gRPC notifier channel
        let (alias_redirected_sender, alias_redirected_receiver) = channel::unbounded();
        let (operation_sender, operation_receiver) = channel::unbounded();
        let internal_app_config = self.config.clone();
        let _intern_pipeline_thread = thread::spawn(move || {
            if let Err(e) = start_internal_pipeline_server(
                internal_app_config,
                (alias_redirected_receiver, operation_receiver),
            ) {
                std::panic::panic_any(OrchestrationError::InternalServerFailed(e));
            }
            warn!("Shutting down internal pipeline server");
        });

        let pipeline_dir = get_pipeline_dir(&self.config);
        let executor = Executor::new(
            &self.config.sources,
            self.config.sql.as_deref(),
            &self.config.endpoints,
            &pipeline_dir,
            running,
        );
        let flags = get_flags(self.config.clone());
        let api_security = get_api_security_config(self.config.clone());
        let settings = CacheSinkSettings::new(get_api_dir(&self.config), flags, api_security);
        let dag_executor = executor.create_dag_executor(
            Some((alias_redirected_sender, operation_sender)),
            get_cache_manager_options(&self.config),
            settings,
            get_executor_options(&self.config),
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
            &self.config.sources,
            self.config.sql.as_deref(),
            &[],
            pipeline_dir.path(),
            running,
        );

        let dag = executor.query(sql, sender)?;
        let dag_schemas = DagSchemas::new(dag)?;

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
            &self.config.sources,
            self.config.sql.as_deref(),
            &self.config.endpoints,
            &pipeline_home_dir,
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
        let dag = builder.build(None, get_cache_manager_options(&self.config), settings)?;
        // Populate schemas.
        DagSchemas::new(dag)?;

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

async fn redirect_cache_endpoints(
    cache_manager: Box<dyn CacheManager>,
    cache_endpoints: Vec<Arc<RoCacheEndpoint>>,
    mut alias_redirected_receiver: Receiver<AliasRedirected>,
) -> Result<(), OrchestrationError> {
    loop {
        let alias_redirected = alias_redirected_receiver
            .recv()
            .await
            .map_err(|e| OrchestrationError::InternalError(Box::new(e)))?;
        for cache_endpoint in &cache_endpoints {
            if cache_endpoint.endpoint().name == alias_redirected.alias {
                cache_endpoint
                    .redirect_cache(&*cache_manager)
                    .map_err(|e| OrchestrationError::InternalError(Box::new(e)))?;
            }
        }
    }
}
