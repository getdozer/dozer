use dozer_api::grpc::internal_grpc::PipelineRequest;
use dozer_core::dag::app::App;
use dozer_types::crossbeam::channel::Sender;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{fs, thread};

use dozer_api::CacheEndpoint;
use dozer_types::models::source::Source;

use crate::pipeline::source_builder::SourceBuilder;
use crate::pipeline::CacheSinkFactory;
use dozer_core::dag::dag::DEFAULT_PORT_HANDLE;
use dozer_core::dag::executor::{DagExecutor, ExecutorOptions};
use dozer_ingestion::ingestion::{IngestionIterator, Ingestor};

use dozer_sql::pipeline::builder::PipelineBuilder;
use dozer_types::parking_lot::RwLock;

use crate::errors::OrchestrationError;

pub struct Executor {
    sources: Vec<Source>,
    cache_endpoints: Vec<CacheEndpoint>,
    home_dir: PathBuf,
    ingestor: Arc<RwLock<Ingestor>>,
    iterator: Arc<RwLock<IngestionIterator>>,
    running: Arc<AtomicBool>,
}
impl Executor {
    pub fn new(
        sources: Vec<Source>,
        cache_endpoints: Vec<CacheEndpoint>,
        ingestor: Arc<RwLock<Ingestor>>,
        iterator: Arc<RwLock<IngestionIterator>>,
        running: Arc<AtomicBool>,
        home_dir: PathBuf,
    ) -> Self {
        Self {
            sources,
            cache_endpoints,
            home_dir,
            ingestor,
            iterator,
            running,
        }
    }

    pub fn run(
        &self,
        notifier: Option<Sender<PipelineRequest>>,
        _running: Arc<AtomicBool>,
    ) -> Result<(), OrchestrationError> {
        let asm = SourceBuilder::build_source_manager(
            self.sources.clone(),
            self.ingestor.clone(),
            self.iterator.clone(),
        );

        let running_wait = self.running.clone();

        let mut app = App::new(asm);

        for cache_endpoint in self.cache_endpoints.iter().cloned() {
            let api_endpoint = cache_endpoint.endpoint.clone();
            let _api_endpoint_name = api_endpoint.name.clone();
            let cache = cache_endpoint.cache;

            let mut pipeline = PipelineBuilder {}
                .build_pipeline(&api_endpoint.sql)
                .map_err(OrchestrationError::SqlStatementFailed)?;

            pipeline.add_sink(
                Arc::new(CacheSinkFactory::new(
                    vec![DEFAULT_PORT_HANDLE],
                    cache,
                    api_endpoint,
                    notifier.clone(),
                )),
                cache_endpoint.endpoint.id(),
            );

            pipeline
                .connect_nodes(
                    "aggregation",
                    Some(DEFAULT_PORT_HANDLE),
                    cache_endpoint.endpoint.id(),
                    Some(DEFAULT_PORT_HANDLE),
                )
                .map_err(OrchestrationError::ExecutionError)?;

            app.add_pipeline(pipeline);
        }

        let dag = app.get_dag().unwrap();

        let path = self.home_dir.join("pipeline");
        fs::create_dir_all(&path).map_err(|_e| OrchestrationError::InternalServerError)?;
        let mut exec = DagExecutor::new(&dag, path.as_path(), ExecutorOptions::default())?;

        exec.start()?;
        // Waiting for Ctrl+C
        while running_wait.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(200));
        }

        exec.stop();
        exec.join()
            .map_err(|_e| OrchestrationError::InternalServerError)?;
        Ok(())
    }
}
