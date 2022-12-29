use super::executor::Executor;
use crate::errors::OrchestrationError;
use crate::utils::{
    get_api_dir, get_api_security_config, get_cache_dir, get_grpc_config, get_pipeline_config,
    get_pipeline_dir, get_rest_config,
};
use crate::Orchestrator;
use dozer_api::auth::{Access, Authorizer};
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
use dozer_ingestion::ingestion::IngestionConfig;
use dozer_ingestion::ingestion::Ingestor;
use dozer_types::crossbeam::channel::{self, unbounded, Sender};
use dozer_types::log::info;
use dozer_types::models::app_config::Config;
use dozer_types::serde_yaml;
use dozer_types::types::Schema;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::{sync::Arc, thread};
use tokio::sync::oneshot;

#[derive(Default, Clone)]
pub struct SimpleOrchestrator {
    pub config: Config,
    pub cache_common_options: CacheCommonOptions,
    pub cache_read_options: CacheReadOptions,
    pub cache_write_options: CacheWriteOptions,
}

impl SimpleOrchestrator {
    pub fn new(config: Config) -> Self {
        Self {
            config,
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
        self.write_internal_config()?;
        // Channel to communicate CtrlC with API Server
        let (tx, rx) = unbounded::<ServerHandle>();
        let running2 = running.clone();
        // gRPC notifier channel
        let cache_dir = get_cache_dir(self.config.to_owned());

        let cache_endpoints: Vec<CacheEndpoint> = self
            .config
            .endpoints
            .iter()
            .map(|e| {
                let mut cache_common_options = self.cache_common_options.clone();
                cache_common_options.set_path(cache_dir.join(e.name.clone()));
                CacheEndpoint {
                    cache: Arc::new(
                        LmdbCache::new(CacheOptions {
                            common: cache_common_options,
                            kind: CacheOptionsKind::ReadOnly(self.cache_read_options.clone()),
                        })
                        .unwrap(),
                    ),
                    endpoint: e.to_owned(),
                }
            })
            .collect();

        let ce2 = cache_endpoints.clone();
        let ce3 = cache_endpoints;

        let rt = tokio::runtime::Runtime::new().expect("Failed to initialize tokio runtime");
        let (sender_shutdown, receiver_shutdown) = oneshot::channel::<()>();
        rt.block_on(async {
            // Initialize API Server
            let rest_config = get_rest_config(self.config.to_owned());
            let security = get_api_security_config(self.config.to_owned());
            tokio::spawn(async move {
                let api_server = rest::ApiServer::new(rest_config, security);
                api_server
                    .run(ce3, tx)
                    .await
                    .expect("Failed to initialize api server")
            });

            // Initialize GRPC Server
            let grpc_config = get_grpc_config(self.config.to_owned());
            let api_dir = get_api_dir(self.config.to_owned());
            let pipeline_config = get_pipeline_config(self.config.to_owned());
            let api_security = get_api_security_config(self.config.to_owned());
            let grpc_server =
                grpc::ApiServer::new(grpc_config, true, api_dir, pipeline_config, api_security);
            tokio::spawn(async move {
                grpc_server
                    .run(ce2, running2.to_owned(), receiver_shutdown)
                    .await
                    .expect("Failed to initialize gRPC server")
            });
        });

        let server_handle = rx.recv().map_err(OrchestrationError::RecvError)?;

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
        self.write_internal_config()?;
        let pipeline_home_dir = get_pipeline_dir(self.config.to_owned());

        // gRPC notifier channel
        let (sender, receiver) = channel::unbounded::<PipelineResponse>();
        let internal_app_config = self.config.to_owned();
        let _intern_pipeline_thread = thread::spawn(move || {
            _ = start_internal_pipeline_server(internal_app_config, receiver);
        });
        // Ingestion channel
        let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());
        let cache_dir = get_cache_dir(self.config.to_owned());

        let cache_endpoints: Vec<CacheEndpoint> = self.get_cache_endpoints(cache_dir);

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
        executor.run(Some(sender))
    }

    fn list_connectors(
        &self,
    ) -> Result<HashMap<String, Vec<(String, Schema)>>, OrchestrationError> {
        Executor::get_tables(&self.config.connections)
    }

    fn generate_token(&self) -> Result<String, OrchestrationError> {
        if let Some(api_config) = self.config.api.to_owned() {
            if let Some(api_security) = api_config.api_security {
                match api_security {
                    dozer_types::models::api_security::ApiSecurity::Jwt(secret) => {
                        let auth = Authorizer::new(secret, None, None);
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

    fn init(&mut self) -> Result<(), OrchestrationError> {
        self.write_internal_config()?;
        let pipeline_home_dir = get_pipeline_dir(self.config.to_owned());
        let api_dir = get_api_dir(self.config.to_owned());

        // Ingestion channel
        let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());
        let cache_dir = get_cache_dir(self.config.to_owned());

        let cache_endpoints: Vec<CacheEndpoint> = self.get_cache_endpoints(cache_dir);

        let sources = self.config.sources.clone();

        let executor = Executor::new(
            sources,
            cache_endpoints.clone(),
            ingestor,
            iterator,
            Arc::new(AtomicBool::new(true)),
            pipeline_home_dir,
        );

        let dag = executor.build_pipeline(None)?;

        let sinks = dag.get_sinks();
        let schema_manager = DagSchemaManager::new(&dag)?;

        let generated_path = api_dir.join("generated");
        if generated_path.exists() {
            fs::remove_dir_all(&generated_path).unwrap();
        }
        fs::create_dir_all(&generated_path).unwrap();

        for (sink_handle, sink) in sinks {
            let schemas = schema_manager
                .get_node_input_schemas(&sink_handle)
                .expect("Failed to get node input schemas");
            sink.prepare(
                schemas.to_owned(),
                generated_path.to_owned(),
                get_api_security_config(self.config.to_owned()),
            )
            .expect("Failed to prepare sink factory");
        }

        info!("Initialized schema");

        Ok(())
    }
}

impl SimpleOrchestrator {
    fn get_cache_endpoints(&self, cache_dir: PathBuf) -> Vec<CacheEndpoint> {
        self.config
            .endpoints
            .iter()
            .map(|e| {
                let mut cache_common_options = self.cache_common_options.clone();
                cache_common_options.set_path(cache_dir.join(e.name.clone()));
                CacheEndpoint {
                    cache: Arc::new(
                        LmdbCache::new(CacheOptions {
                            common: cache_common_options,
                            kind: CacheOptionsKind::Write(self.cache_write_options.clone()),
                        })
                        .unwrap(),
                    ),
                    endpoint: e.to_owned(),
                }
            })
            .collect()
    }
}
