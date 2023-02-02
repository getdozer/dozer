use super::executor::Executor;
use crate::console_helper::get_colored_text;
use crate::errors::OrchestrationError;
use crate::pipeline::CacheSinkSettings;
use crate::utils::{
    get_api_dir, get_api_security_config, get_cache_dir, get_flags, get_grpc_config,
    get_pipeline_config, get_pipeline_dir, get_rest_config,
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
    rest, CacheEndpoint,
};
use dozer_cache::cache::{CacheCommonOptions, CacheOptions, CacheReadOptions, CacheWriteOptions};
use dozer_cache::cache::{CacheOptionsKind, LmdbCache};
use dozer_core::dag::dag_schemas::DagSchemaManager;
use dozer_core::dag::errors::ExecutionError::InternalError;
use dozer_ingestion::ingestion::IngestionConfig;
use dozer_ingestion::ingestion::Ingestor;
use dozer_sql::pipeline::builder::statement_to_pipeline;
use dozer_types::crossbeam::channel::{self, unbounded, Sender};
use dozer_types::log::{info, warn};
use dozer_types::models::api_config::ApiConfig;
use dozer_types::models::api_endpoint::ApiEndpoint;
use dozer_types::models::app_config::Config;
use dozer_types::prettytable::{row, Table};
use dozer_types::serde_yaml;
use dozer_types::tracing::error;
use dozer_types::types::{Operation, Schema, SchemaWithChangesType};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::{sync::Arc, thread};
use tokio::sync::{broadcast, oneshot};

#[derive(Default, Clone)]
pub struct SimpleOrchestrator {
    pub config: Config,
    pub cache_common_options: CacheCommonOptions,
    pub cache_read_options: CacheReadOptions,
    pub cache_write_options: CacheWriteOptions,
}

impl SimpleOrchestrator {
    pub fn new(config: &Config) -> Self {
        Self {
            config: config.clone(),
            ..Default::default()
        }
    }
    fn write_internal_config(&self) -> Result<(), OrchestrationError> {
        let path = Path::new(&self.config.home_dir).join("internal_config");
        if path.exists() {
            fs::remove_dir_all(&path).unwrap();
        }
        fs::create_dir_all(&path).unwrap();
        let yaml_path = path.join("config.yaml");
        let f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(yaml_path)
            .expect("Couldn't open file");
        let api_config = self.config.api.to_owned().unwrap_or_default();
        let api_internal = api_config.to_owned().api_internal.unwrap_or_default();
        let pipeline_internal = api_config.pipeline_internal.unwrap_or_default();
        let mut internal_content = serde_yaml::Value::default();
        internal_content["api_internal"] = serde_yaml::to_value(api_internal)
            .map_err(OrchestrationError::FailedToWriteConfigYaml)?;
        internal_content["pipeline_internal"] = serde_yaml::to_value(pipeline_internal)
            .map_err(OrchestrationError::FailedToWriteConfigYaml)?;
        serde_yaml::to_writer(f, &internal_content)
            .map_err(OrchestrationError::FailedToWriteConfigYaml)?;
        Ok(())
    }
}

impl Orchestrator for SimpleOrchestrator {
    fn run_api(&mut self, running: Arc<AtomicBool>) -> Result<(), OrchestrationError> {
        // Channel to communicate CtrlC with API Server
        let (tx, rx) = unbounded::<ServerHandle>();
        // gRPC notifier channel
        let cache_dir = get_cache_dir(self.config.to_owned());

        // Flags
        let flags = self.config.flags.clone().unwrap_or_default();

        let mut cache_endpoints = vec![];
        for ce in &self.config.endpoints {
            let mut cache_common_options = self.cache_common_options.clone();
            cache_common_options.set_path(cache_dir.join(ce.name.clone()));
            cache_endpoints.push(CacheEndpoint {
                cache: Arc::new(
                    LmdbCache::new(CacheOptions {
                        common: cache_common_options,
                        kind: CacheOptionsKind::ReadOnly(self.cache_read_options.clone()),
                    })
                    .map_err(OrchestrationError::CacheInitFailed)?,
                ),
                endpoint: ce.to_owned(),
            });
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
            let pipeline_config = get_pipeline_config(self.config.to_owned());

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

            let api_dir = get_api_dir(self.config.to_owned());
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
        let pipeline_home_dir = get_pipeline_dir(self.config.to_owned());
        // gRPC notifier channel
        let (sender, receiver) = channel::unbounded::<PipelineResponse>();
        let internal_app_config = self.config.to_owned();
        let _intern_pipeline_thread = thread::spawn(move || {
            if let Err(e) = start_internal_pipeline_server(internal_app_config, receiver) {
                std::panic::panic_any(OrchestrationError::InternalServerFailed(e));
            }
            warn!("Shutting down internal pipeline server");
        });
        // Ingestion channel
        let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());
        let cache_dir = get_cache_dir(self.config.to_owned());

        let cache_endpoints: Vec<CacheEndpoint> = self.get_cache_endpoints(cache_dir)?;

        if let Some(api_notifier) = api_notifier {
            api_notifier
                .send(true)
                .expect("Failed to notify API server");
        }

        let sources = self.config.sources.clone();

        let executor = Executor::new(
            sources,
            cache_endpoints,
            ingestor,
            iterator,
            running,
            pipeline_home_dir,
        );
        let flags = get_flags(self.config.clone());
        let api_security = get_api_security_config(self.config.clone());
        let settings = CacheSinkSettings::new(flags, api_security);
        executor.run(Some(sender), settings)
    }

    fn list_connectors(
        &self,
    ) -> Result<HashMap<String, Vec<SchemaWithChangesType>>, OrchestrationError> {
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
        // Ingestion channel
        let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());

        let sources = self.config.sources.clone();

        let pipeline_dir = tempdir::TempDir::new("query4")
            .map_err(|e| OrchestrationError::InternalError(Box::new(e)))?;
        let executor = Executor::new(
            sources,
            vec![],
            ingestor,
            iterator,
            running,
            pipeline_dir.into_path(),
        );

        let dag = executor.query(sql, sender)?;
        let schema_manager = DagSchemaManager::new(&dag)?;
        let streaming_sink_handle = dag.sinks().next().expect("Sink is expected").0;
        let (schema, _ctx) = schema_manager
            .get_node_input_schemas(streaming_sink_handle)?
            .values()
            .next()
            .expect("schema is expected")
            .clone();
        Ok(schema)
    }
    fn migrate(&mut self, force: bool) -> Result<(), OrchestrationError> {
        self.write_internal_config()
            .map_err(|e| InternalError(Box::new(e)))?;
        let pipeline_home_dir = get_pipeline_dir(self.config.to_owned());
        let api_dir = get_api_dir(self.config.to_owned());
        let cache_dir = get_cache_dir(self.config.to_owned());

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

        info!(
            "Home dir: {}",
            get_colored_text(&self.config.home_dir, "35")
        );
        if let Some(api_config) = &self.config.api {
            print_api_config(api_config)
        }

        print_api_endpoints(&self.config.endpoints);
        validate_endpoints(&self.config.endpoints)?;

        // Ingestion channel
        let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());

        let cache_endpoints: Vec<CacheEndpoint> = self.get_cache_endpoints(cache_dir)?;

        let sources = self.config.sources.clone();

        let executor = Executor::new(
            sources,
            cache_endpoints,
            ingestor,
            iterator,
            Arc::new(AtomicBool::new(true)),
            pipeline_home_dir.clone(),
        );

        // Api Path
        let generated_path = api_dir.join("generated");
        if !generated_path.exists() {
            fs::create_dir_all(generated_path.clone()).map_err(|e| InternalError(Box::new(e)))?;
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
        let settings = CacheSinkSettings::new(flags, api_security);
        let dag = executor.build_pipeline(None, generated_path.clone(), settings)?;
        let schema_manager = DagSchemaManager::new(&dag)?;
        // Every sink will initialize its schema in sink and also in a proto file.
        schema_manager.prepare()?;

        let mut resources = Vec::new();
        for e in &self.config.endpoints {
            resources.push(e.name.clone());
        }

        // Copy common service to be included in descriptor.
        resources.push("common".to_string());

        ProtoGenerator::copy_common(&generated_path)
            .map_err(|e| OrchestrationError::InternalError(Box::new(e)))?;
        // Generate a descriptor based on all proto files generated within sink.
        ProtoGenerator::generate_descriptor(&generated_path, resources)
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
}

impl SimpleOrchestrator {
    fn get_cache_endpoints(
        &self,
        cache_dir: PathBuf,
    ) -> Result<Vec<CacheEndpoint>, OrchestrationError> {
        let mut cache_endpoints = Vec::new();
        for e in &self.config.endpoints {
            let mut cache_common_options = self.cache_common_options.clone();
            cache_common_options.set_path(cache_dir.join(e.name.clone()));
            cache_endpoints.push(CacheEndpoint {
                cache: Arc::new(
                    LmdbCache::new(CacheOptions {
                        common: cache_common_options,
                        kind: CacheOptionsKind::Write(self.cache_write_options.clone()),
                    })
                    .map_err(|e| OrchestrationError::InternalError(Box::new(e)))?,
                ),
                endpoint: e.to_owned(),
            })
        }
        Ok(cache_endpoints)
    }
}

pub fn validate_endpoints(endpoints: &Vec<ApiEndpoint>) -> Result<(), OrchestrationError> {
    let mut is_all_valid = true;
    for endpoint in endpoints {
        endpoint.sql.as_ref().map_or_else(
            || {
                info!(
                    "[Endpoints][{}] {} Endpoint validation completed",
                    endpoint.name,
                    get_colored_text("✓", "32")
                );
            },
            |sql| {
                statement_to_pipeline(sql).map_or_else(
                    |e| {
                        is_all_valid = false;
                        error!(
                            "[Endpoints][{}] {} Endpoint validation error: {}",
                            endpoint.name,
                            get_colored_text("X", "31"),
                            e
                        );
                    },
                    |_| {
                        info!(
                            "[Endpoints][{}] {} Endpoint validation completed",
                            endpoint.name,
                            get_colored_text("✓", "32")
                        );
                    },
                );
            },
        );
    }

    if is_all_valid {
        Ok(())
    } else {
        Err(OrchestrationError::PipelineValidationError)
    }
}

fn print_api_config(api_config: &ApiConfig) {
    info!("[API] {}", get_colored_text("Configuration", "35"));
    let mut table_parent = Table::new();

    table_parent.add_row(row!["Type", "IP", "Port"]);
    if let Some(rest_config) = &api_config.rest {
        table_parent.add_row(row!["REST", rest_config.host, rest_config.port]);
    }

    if let Some(grpc_config) = &api_config.grpc {
        table_parent.add_row(row!["GRPC", grpc_config.host, grpc_config.port]);
    }

    table_parent.printstd();
}

fn print_api_endpoints(endpoints: &Vec<ApiEndpoint>) {
    info!("[API] {}", get_colored_text("Endpoints", "35"));
    let mut table_parent = Table::new();

    table_parent.add_row(row!["Path", "Name", "Sql"]);
    for endpoint in endpoints {
        let sql = endpoint
            .sql
            .as_ref()
            .map_or("-".to_string(), |sql| sql.clone());
        table_parent.add_row(row![endpoint.path, endpoint.name, sql]);
    }

    table_parent.printstd();
}
