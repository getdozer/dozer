use super::executor::Executor;
use crate::errors::OrchestrationError;
use crate::utils::{
    get_api_dir, get_cache_dir, get_grpc_config, get_pipeline_config, get_pipeline_dir,
    get_rest_config,
};
use crate::Orchestrator;
use dozer_api::{
    actix_web::dev::ServerHandle,
    grpc::{
        self,
        internal_grpc::PipelineResponse,
    },
    rest, CacheEndpoint,
};
use dozer_cache::cache::{Cache, CacheCommonOptions, CacheOptions, CacheReadOptions, CacheWriteOptions};
use dozer_cache::cache::{CacheOptionsKind, LmdbCache};
use dozer_ingestion::ingestion::IngestionIterator;
use dozer_ingestion::ingestion::Ingestor;
use dozer_types::crossbeam::channel::{unbounded, Sender};
use dozer_types::models::app_config::Config;
use dozer_types::serde_yaml;
use dozer_types::types::{IndexDefinition, Schema};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::oneshot;
use dozer_core::dag::dag::Dag;
use dozer_core::dag::dag_schemas::{DagSchemaManager, NodeSchemas};
use dozer_core::dag::node::PortHandle;
use dozer_types::parking_lot::RwLock;

#[derive(Default, Clone)]
pub struct SimpleOrchestrator {
    pub config: Config,
    pub cache_common_options: CacheCommonOptions,
    pub cache_read_options: CacheReadOptions,
    pub cache_write_options: CacheWriteOptions,
    pub cache_endpoints: Vec<CacheEndpoint>,
    pub parent_dag: Dag,
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
    fn init(
        &mut self,
        running: Arc<AtomicBool>,
        api_notifier: Option<Sender<bool>>,
        sender: Sender<PipelineResponse>,
        ingestor: Arc<RwLock<Ingestor>>,
        iterator: Arc<RwLock<IngestionIterator>>,
    ) -> Result<(), OrchestrationError> {
        self.write_internal_config()?;

        if let Some(api_notifier) = api_notifier {
            api_notifier
                .send(true)
                .expect("Failed to notify API server");
        }

        let executor = Executor::new(
            self.config.sources.clone(),
            self.cache_endpoints.clone(),
            ingestor,
            iterator,
            running,
            get_pipeline_dir(self.config.to_owned()),
        );

        self.parent_dag = executor.init_dag(Some(sender)).expect("Failed to initialize dag");

        // parent_dag is ready
        let schema_manager = DagSchemaManager::new(&self.parent_dag)?;
        let schema_map = schema_manager.get_all_schemas().clone();
        let node_schemas = schema_map.values().cloned().collect::<Vec<NodeSchemas>>();

        self.cache_endpoints = self
            .config
            .endpoints
            .iter()
            .enumerate()
            .map(|(i, e)| {
                let mut cache_common_options = self.cache_common_options.clone();
                cache_common_options.set_path(get_cache_dir(self.config.to_owned()).join(e.name.clone()));
                let cache_options = CacheOptions {
                    common: cache_common_options,
                    kind: CacheOptionsKind::Write(self.cache_write_options.clone()),
                };
                let lmdb_cache = LmdbCache::new(cache_options).expect("Failed to initialize lmdb cache");

                let schema = node_schemas
                    .get(0)
                    .unwrap()
                    .output_schemas
                    .get(&(i as PortHandle))
                    .expect("Error getting schema for endpoint initialization");

                let secondary_indexes: &[IndexDefinition] = &schema
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(idx, _f)| IndexDefinition::SortedInverted(vec![idx]))
                    .collect::<Vec<IndexDefinition>>();

                lmdb_cache.insert_schema(e.name.as_str(), schema, secondary_indexes)
                    .expect("Failed to insert schema into cache endpoint");

                CacheEndpoint {
                    cache: Arc::from(lmdb_cache),
                    endpoint: e.to_owned(),
                }
            })
            .collect();

        Ok(())
    }

    fn run_api(&mut self, running: Arc<AtomicBool>) -> Result<(), OrchestrationError> {
        // Channel to communicate CtrlC with API Server
        let (tx, rx) = unbounded::<ServerHandle>();
        let running2 = running.clone();

        let ce2 = self.cache_endpoints.clone();
        let ce3 = self.cache_endpoints.clone();

        let rt = tokio::runtime::Runtime::new().expect("Failed to initialize tokio runtime");
        let (sender_shutdown, receiver_shutdown) = oneshot::channel::<()>();
        rt.block_on(async {
            // Initialize API Server
            let rest_config = get_rest_config(self.config.to_owned());
            tokio::spawn(async move {
                let api_server = rest::ApiServer::new(rest_config);
                api_server
                    .run(ce3, tx)
                    .await
                    .expect("Failed to initialize api server")
            });

            // Initialize GRPC Server
            let grpc_config = get_grpc_config(self.config.to_owned());
            let api_dir = get_api_dir(self.config.to_owned());
            let pipeline_config = get_pipeline_config(self.config.to_owned());
            let grpc_server = grpc::ApiServer::new(grpc_config, true, api_dir, pipeline_config);
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
        ingestor: Arc<RwLock<Ingestor>>,
        iterator: Arc<RwLock<IngestionIterator>>,
    ) -> Result<(), OrchestrationError> {
        if let Some(api_notifier) = api_notifier {
            api_notifier
                .send(true)
                .expect("Failed to notify API server");
        }

        let executor = Executor::new(
            self.config.sources.clone(),
            self.cache_endpoints.clone(),
            ingestor,
            iterator,
            running,
            get_pipeline_dir(self.config.to_owned()),
        );

        executor.run_dag(&self.parent_dag)
    }

    fn list_connectors(
        &self,
    ) -> Result<HashMap<String, Vec<(String, Schema)>>, OrchestrationError> {
        Executor::get_tables(&self.config.connections)
    }
}
