use super::executor::Executor;
use crate::console_helper::get_colored_text;
use crate::errors::{DeployError, OrchestrationError};
use crate::pipeline::{LogSinkSettings, PipelineBuilder};
use crate::shutdown::ShutdownReceiver;
use crate::simple::helper::validate_config;
use crate::utils::{
    get_api_security_config, get_cache_manager_options, get_executor_options,
    get_file_buffer_capacity, get_grpc_config, get_rest_config,
};
use crate::{flatten_join_handle, CloudOrchestrator, Orchestrator};
use dozer_api::auth::{Access, Authorizer};
use dozer_api::generator::protoc::generator::ProtoGenerator;
use dozer_api::{grpc, rest, CacheEndpoint};
use dozer_cache::cache::LmdbRwCacheManager;
use dozer_cache::dozer_log::home_dir::HomeDir;
use dozer_cache::dozer_log::schemas::write_schemas;
use dozer_core::app::AppPipeline;
use dozer_core::dag_schemas::DagSchemas;

use dozer_core::errors::ExecutionError;
use dozer_ingestion::connectors::{SourceSchema, TableInfo};
use dozer_sql::pipeline::builder::statement_to_pipeline;
use dozer_sql::pipeline::errors::PipelineError;
use dozer_types::crossbeam::channel::{self, Sender};
use dozer_types::grpc_types::cloud::dozer_cloud_client::DozerCloudClient;
use dozer_types::grpc_types::cloud::{CreateAppRequest, StartRequest};
use dozer_types::indicatif::MultiProgress;
use dozer_types::log::{info, warn};
use dozer_types::models::app_config::Config;
use dozer_types::tracing::error;

use crate::cli::types::Cloud;
use dozer_api::grpc::internal::internal_pipeline_server::start_internal_pipeline_server;
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
        );
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
        if home_dir.exists() {
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
            &home_dir,
            &self.config.connections,
            &self.config.sources,
            self.config.sql.as_deref(),
            &self.config.endpoints,
            self.multi_pb.clone(),
        );

        home_dir
            .create_dir_all(
                self.config
                    .endpoints
                    .iter()
                    .map(|endpoint| endpoint.name.as_str()),
            )
            .map_err(|(path, error)| OrchestrationError::FailedToCreateDir(path, error))?;

        let settings = LogSinkSettings {
            file_buffer_capacity: get_file_buffer_capacity(&self.config),
        };
        let dag = builder.build(self.runtime.clone(), settings, None)?;
        // Populate schemas.
        let dag_schemas = DagSchemas::new(dag)?;

        // Write schemas to pipeline_dir and generate proto files.
        let schemas = write_schemas(
            dag_schemas.get_sink_schemas(),
            &home_dir,
            &self.config.endpoints,
        )
        .map_err(OrchestrationError::FailedToWriteSchema)?;
        let api_config = self.config.api.clone().unwrap_or_default();
        for (endpoint_name, schema) in &schemas {
            let endpoint_api_dir = home_dir.get_endpoint_api_dir(endpoint_name);

            ProtoGenerator::generate(
                &endpoint_api_dir,
                endpoint_name,
                schema,
                &api_config.api_security,
                &self.config.flags,
            )?;

            let mut resources = Vec::new();
            resources.push(endpoint_name);

            let common_resources = ProtoGenerator::copy_common(&endpoint_api_dir)
                .map_err(|e| OrchestrationError::InternalError(Box::new(e)))?;

            // Copy common service to be included in descriptor.
            resources.extend(common_resources.iter());

            // Generate a descriptor based on all proto files generated within sink.
            let descriptor_path = home_dir.get_endpoint_descriptor_path(endpoint_name);
            ProtoGenerator::generate_descriptor(&endpoint_api_dir, &descriptor_path, &resources)
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
        let pipeline_thread = thread::spawn(move || dozer_pipeline.run_apps(shutdown, Some(tx)));

        // Wait for pipeline to initialize caches before starting api server
        rx.recv().unwrap();

        dozer_api.run_api(shutdown_api)?;

        // wait for pipeline thread to shutdown gracefully
        pipeline_thread.join().unwrap()
    }
}

impl CloudOrchestrator for SimpleOrchestrator {
    // TODO: Deploy Dozer application using local Dozer configuration
    fn deploy(&mut self, cloud: Cloud, config_path: String) -> Result<(), OrchestrationError> {
        let target_url = cloud.target_url;
        // let username = match deploy.username {
        //     Some(u) => u,
        //     None => String::new(),
        // };
        // let _password = match deploy.password {
        //     Some(p) => p,
        //     None => String::new(),
        // };
        info!("Deployment target url: {:?}", target_url);
        // info!("Authenticating for username: {:?}", username);
        // info!("Local dozer configuration path: {:?}", config_path);
        // getting local dozer config file
        let config_content = fs::read_to_string(&config_path)
            .map_err(|e| DeployError::CannotReadConfig(config_path.into(), e))?;
        // calling the target url with the config fetched
        self.runtime.block_on(async move {
            // 1. CREATE application
            let mut client: DozerCloudClient<tonic::transport::Channel> =
                DozerCloudClient::connect(target_url).await?;
            let response = client
                .create_application(CreateAppRequest {
                    config: config_content,
                })
                .await?
                .into_inner();
            info!("Application created with id: {:?}", response.id);
            // 2. START application
            info!("Deploying application");
            client.start_dozer(StartRequest { id: response.id }).await?;
            info!("Deployed");
            Ok::<(), DeployError>(())
        })?;
        Ok(())
    }

    fn list(&mut self, _cloud: Cloud) -> Result<(), OrchestrationError> {
        todo!()
    }

    fn status(&mut self, _cloud: Cloud) -> Result<(), OrchestrationError> {
        todo!()
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
