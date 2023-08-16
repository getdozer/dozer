use dozer_api::grpc::internal::internal_pipeline_server::LogEndpoint;
use dozer_cache::dozer_log::camino::Utf8Path;
use dozer_cache::dozer_log::dyn_clone;
use dozer_cache::dozer_log::home_dir::{BuildPath, HomeDir};
use dozer_cache::dozer_log::replication::{create_data_storage, Log};
use dozer_cache::dozer_log::storage::Storage;
use dozer_core::errors::ExecutionError;
use dozer_types::models::api_endpoint::ApiEndpoint;
use dozer_types::models::app_config::DataStorage;
use dozer_types::parking_lot::Mutex;
use tokio::runtime::Runtime;

use std::sync::{atomic::AtomicBool, Arc};

use dozer_types::models::source::Source;

use crate::pipeline::PipelineBuilder;
use crate::shutdown::ShutdownReceiver;
use dozer_core::executor::{DagExecutor, ExecutorOptions};

use dozer_types::indicatif::MultiProgress;

use dozer_types::models::connection::Connection;

use crate::errors::OrchestrationError;

pub struct Executor<'a> {
    connections: &'a [Connection],
    sources: &'a [Source],
    sql: Option<&'a str>,
    checkpoint_storage: Box<dyn Storage>,
    checkpoint_prefix: String,
    /// `ApiEndpoint` and its log.
    endpoint_and_logs: Vec<(ApiEndpoint, LogEndpoint)>,
    multi_pb: MultiProgress,
}

impl<'a> Executor<'a> {
    pub async fn new(
        home_dir: &'a HomeDir,
        connections: &'a [Connection],
        sources: &'a [Source],
        sql: Option<&'a str>,
        api_endpoints: &'a [ApiEndpoint],
        storage_config: DataStorage,
        multi_pb: MultiProgress,
    ) -> Result<Executor<'a>, OrchestrationError> {
        let build_path = home_dir
            .find_latest_build_path()
            .map_err(|(path, error)| OrchestrationError::FileSystem(path.into(), error))?
            .ok_or(OrchestrationError::NoBuildFound)?;
        let (checkpoint_storage, checkpoint_prefix) =
            create_data_storage(storage_config, build_path.data_dir.to_string())
                .await
                .map_err(ExecutionError::ObjectStorage)?;

        let mut endpoint_and_logs = vec![];
        for endpoint in api_endpoints {
            let log_endpoint = create_log_endpoint(
                &build_path,
                &endpoint.name,
                &*checkpoint_storage,
                &checkpoint_prefix,
            )
            .await?;
            endpoint_and_logs.push((endpoint.clone(), log_endpoint));
        }

        Ok(Executor {
            connections,
            sources,
            sql,
            checkpoint_storage,
            checkpoint_prefix,
            endpoint_and_logs,
            multi_pb,
        })
    }

    pub fn endpoint_and_logs(&self) -> &[(ApiEndpoint, LogEndpoint)] {
        &self.endpoint_and_logs
    }

    pub async fn create_dag_executor(
        &self,
        runtime: &Arc<Runtime>,
        executor_options: ExecutorOptions,
        shutdown: ShutdownReceiver,
    ) -> Result<DagExecutor, OrchestrationError> {
        let builder = PipelineBuilder::new(
            self.connections,
            self.sources,
            self.sql,
            self.endpoint_and_logs
                .iter()
                .map(|(endpoint, log)| (endpoint.clone(), Some(log.log.clone())))
                .collect(),
            self.multi_pb.clone(),
        );

        let dag = builder.build(runtime, shutdown).await?;
        let exec = DagExecutor::new(
            dag,
            dyn_clone::clone_box(&*self.checkpoint_storage),
            self.checkpoint_prefix.clone(),
            executor_options,
        )
        .await?;

        Ok(exec)
    }
}

pub fn run_dag_executor(
    dag_executor: DagExecutor,
    running: Arc<AtomicBool>,
) -> Result<(), OrchestrationError> {
    let join_handle = dag_executor.start(running)?;
    join_handle
        .join()
        .map_err(OrchestrationError::ExecutionError)
}

async fn create_log_endpoint(
    build_path: &BuildPath,
    endpoint_name: &str,
    checkpoint_storage: &dyn Storage,
    checkpoint_prefix: &str,
) -> Result<LogEndpoint, OrchestrationError> {
    let endpoint_path = build_path.get_endpoint_path(endpoint_name);

    let schema_string = tokio::fs::read_to_string(&endpoint_path.schema_path)
        .await
        .map_err(|e| OrchestrationError::FileSystem(endpoint_path.schema_path.into(), e))?;

    let descriptor_bytes = tokio::fs::read(&build_path.descriptor_path)
        .await
        .map_err(|e| {
            OrchestrationError::FileSystem(build_path.descriptor_path.clone().into(), e)
        })?;

    let log_prefix = AsRef::<Utf8Path>::as_ref(checkpoint_prefix)
        .join(&endpoint_path.log_dir_relative_to_data_dir);
    let log = Log::new(checkpoint_storage, log_prefix.into(), false).await?;
    let log = Arc::new(Mutex::new(log));

    Ok(LogEndpoint {
        build_id: build_path.id.clone(),
        schema_string,
        descriptor_bytes,
        log,
    })
}
