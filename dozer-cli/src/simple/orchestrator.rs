use super::executor::{run_dag_executor, Executor};
use super::Contract;
use crate::errors::{BuildError, OrchestrationError};
use crate::home_dir::{BuildId, HomeDir};
use crate::pipeline::connector_source::ConnectorSourceFactoryError;
use crate::pipeline::PipelineBuilder;
use crate::simple::build;
use crate::simple::helper::validate_config;
use crate::utils::get_executor_options;

use crate::flatten_join_handle;
use camino::Utf8PathBuf;
use dozer_core::app::AppPipeline;
use dozer_core::dag_schemas::DagSchemas;
use dozer_core::event::EventHub;
use dozer_core::shutdown::ShutdownReceiver;
use dozer_tracing::DozerMonitorContext;
use dozer_types::constants::LOCK_FILE;
use futures::future::{select, Either};

use crate::console_helper::get_colored_text;
use crate::console_helper::GREEN;
use crate::console_helper::PURPLE;
use crate::console_helper::RED;
use dozer_core::errors::ExecutionError;
use dozer_ingestion::{get_connector, SourceSchema, TableInfo};
use dozer_sql::builder::{statement_to_pipeline, UdfRegistry};
use dozer_sql::errors::PipelineError;
use dozer_types::log::info;
use dozer_types::models::config::{default_home_dir, Config};
use dozer_types::parking_lot::Mutex;
use dozer_types::tracing::error;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use std::collections::{HashMap, HashSet};
use std::fs;

use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;

pub struct SimpleOrchestrator {
    pub base_directory: Utf8PathBuf,
    pub config: Config,
    pub runtime: Arc<Runtime>,
    pub labels: DozerMonitorContext,
    udfs: Mutex<UdfRegistry>,
}

impl Clone for SimpleOrchestrator {
    fn clone(&self) -> Self {
        Self {
            base_directory: self.base_directory.clone(),
            config: self.config.clone(),
            runtime: self.runtime.clone(),
            labels: self.labels.clone(),
            // Share the immutable snapshot, but not a mutable cache slot: changing
            // one clone's public config must not invalidate another clone's cache.
            udfs: Mutex::new(self.udfs.lock().clone()),
        }
    }
}

impl SimpleOrchestrator {
    pub fn new(
        base_directory: Utf8PathBuf,
        config: Config,
        runtime: Arc<Runtime>,
        labels: DozerMonitorContext,
    ) -> Self {
        let udfs = Mutex::new(UdfRegistry::from(config.udfs.clone()));
        Self {
            base_directory,
            config,
            runtime,
            labels,
            udfs,
        }
    }

    // Builds, schema propagation and execution share one immutable module snapshot.
    // A changed configuration gets a new registry, never a mismatched old module set.
    pub(crate) fn udf_registry(&self) -> UdfRegistry {
        let mut registry = self.udfs.lock();
        if registry.configs() != self.config.udfs.as_slice() {
            *registry = UdfRegistry::from(self.config.udfs.clone());
        }
        registry.clone()
    }

    pub fn home_dir(&self) -> Utf8PathBuf {
        self.base_directory.join(
            self.config
                .home_dir
                .clone()
                .unwrap_or_else(default_home_dir),
        )
    }

    pub fn lockfile_path(&self) -> Utf8PathBuf {
        lockfile_path(self.base_directory.clone())
    }

    pub async fn run_apps(
        &self,
        shutdown: ShutdownReceiver,
        api_notifier: Option<oneshot::Sender<()>>,
    ) -> Result<(), OrchestrationError> {
        let executor = Executor::new(
            &self.config.connections,
            &self.config.sources,
            self.config.sql.as_deref(),
            &self.config.sinks,
            self.labels.clone(),
            self.udf_registry(),
        )
        .await?;
        let dag_executor = executor
            .create_dag_executor(
                &self.runtime,
                get_executor_options(&self.config),
                shutdown.clone(),
                self.config.flags.clone(),
            )
            .await?;

        if let Some(api_notifier) = api_notifier {
            api_notifier.send(()).expect("Failed to notify API server");
        }

        let labels = self.labels.clone();
        let runtime_clone = self.runtime.clone();
        let shutdown_clone = shutdown.clone();
        let pipeline_future = self.runtime.spawn_blocking(move || {
            run_dag_executor(&runtime_clone, dag_executor, shutdown_clone, labels)
        });

        let mut futures = FuturesUnordered::new();
        futures.push(flatten_join_handle(pipeline_future).boxed());

        while let Some(result) = futures.next().await {
            result?;
        }
        Ok(())
    }

    #[allow(clippy::type_complexity)]
    pub async fn list_connectors(
        &self,
        connections: HashSet<String>,
    ) -> Result<HashMap<String, (Vec<TableInfo>, Vec<SourceSchema>)>, OrchestrationError> {
        let mut schema_map = HashMap::new();
        for connection in self
            .config
            .connections
            .iter()
            .filter(|conn| connections.contains(&conn.name))
        {
            // We're not really going to start ingestion, so passing `None` as state here is OK.
            let mut connector = get_connector(
                self.runtime.clone(),
                EventHub::new(1),
                connection.clone(),
                None,
            )
            .map_err(|e| ConnectorSourceFactoryError::Connector(e.into()))?;
            let schema_tuples = connector
                .list_all_schemas()
                .await
                .map_err(ConnectorSourceFactoryError::Connector)?;
            schema_map.insert(connection.name.clone(), schema_tuples);
        }

        Ok(schema_map)
    }

    pub async fn build(
        &self,
        force: bool,
        shutdown: ShutdownReceiver,
        locked: bool,
    ) -> Result<(), OrchestrationError> {
        let home_dir = self.home_dir();
        let home_dir = HomeDir::new(home_dir);

        info!(
            "Initializing app: {}",
            get_colored_text(&self.config.app_name, PURPLE)
        );
        if force {
            self.clean()?;
        }
        validate_config(&self.config)?;

        let builder = PipelineBuilder::new(
            &self.config.connections,
            &self.config.sources,
            self.config.sql.as_deref(),
            &self.config.sinks,
            self.labels.clone(),
            self.config.flags.clone(),
            self.udf_registry(),
        );
        let dag = builder.build(&self.runtime, shutdown).await?;
        // Populate schemas.
        let dag_schemas = DagSchemas::new(dag).await?;

        // Get current contract.
        let version = self.config.version as usize;

        let contract = build::Contract::new(version, &dag_schemas, &self.config.connections)?;

        let contract_path = self.lockfile_path();
        if locked {
            let existing_contract = Contract::deserialize(contract_path.as_std_path()).ok();
            let Some(existing_contract) = existing_contract.as_ref() else {
                return Err(OrchestrationError::LockedNoLockFile);
            };

            if &contract != existing_contract {
                return Err(OrchestrationError::LockedOutdatedLockfile);
            }
        }

        home_dir
            .create_build_dir_all(BuildId::first())
            .map_err(|(path, error)| BuildError::FileSystem(path.into(), error))?;

        contract.serialize(contract_path.as_std_path())?;

        Ok(())
    }

    // Cleaning the entire folder as there will be inconsistencies
    // between pipeline, cache and generated proto files.
    pub fn clean(&self) -> Result<(), OrchestrationError> {
        let home_dir = self.home_dir();
        if home_dir.exists() {
            fs::remove_dir_all(&home_dir)
                .map_err(|e| ExecutionError::FileSystemError(home_dir.into_std_path_buf(), e))?;
        };

        Ok(())
    }

    pub async fn run_all(
        &self,
        shutdown: ShutdownReceiver,
        locked: bool,
    ) -> Result<(), OrchestrationError> {
        let (tx, rx) = oneshot::channel::<()>();

        self.build(false, shutdown.clone(), locked).await?;

        let dozer_pipeline = self.clone();
        let pipeline_shutdown = shutdown.clone();
        let pipeline_future =
            async move { dozer_pipeline.run_apps(pipeline_shutdown, Some(tx)).await }.boxed();

        match select(rx, pipeline_future).await {
            Either::Left((result, pipeline_future)) => {
                if result.is_err() {
                    // Pipeline panics before ready. Propagate the panic.
                    pipeline_future.await.unwrap();
                    unreachable!("we must have panicked");
                } else {
                    Ok(())
                }
            }
            Either::Right((result, _)) => result,
        }
    }
}

pub fn validate_sql(sql: String, runtime: Arc<Runtime>) -> Result<(), PipelineError> {
    statement_to_pipeline(
        &sql,
        &mut AppPipeline::new_with_default_flags(),
        None,
        vec![],
        runtime,
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

pub fn lockfile_path(base_directory: Utf8PathBuf) -> Utf8PathBuf {
    base_directory.join(LOCK_FILE)
}

#[cfg(all(test, feature = "wasm"))]
mod wasm_tests {
    use super::*;
    use dozer_types::models::udf_config::{UdfConfig, UdfType, WasmConfig};

    #[test]
    fn clones_retain_snapshots_when_one_configuration_changes() {
        let directory = tempfile::tempdir().unwrap();
        let module = directory.path().join("my_function.wasm");
        fs::write(
            &module,
            include_bytes!("../../../examples/wasm-udf/udfs/wasm/my_function.wasm"),
        )
        .unwrap();
        let config = Config {
            udfs: vec![UdfConfig {
                name: "my_function".into(),
                config: UdfType::Wasm(WasmConfig {
                    path: module.to_str().unwrap().into(),
                }),
            }],
            ..Default::default()
        };
        let runtime = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        );
        let original = SimpleOrchestrator::new(
            Utf8PathBuf::from_path_buf(directory.path().to_owned()).unwrap(),
            config.clone(),
            runtime.clone(),
            Default::default(),
        );
        original.udf_registry().prepare().unwrap();
        let mut changed = original.clone();
        let active_snapshot = changed.udf_registry();
        fs::remove_file(&module).unwrap();
        changed.udf_registry().prepare().unwrap();
        if let UdfType::Wasm(wasm) = &mut changed.config.udfs[0].config {
            wasm.path.push_str("-other");
        }
        assert!(changed.udf_registry().prepare().is_err());
        // Neither the original application nor an active old pipeline may reload.
        original.udf_registry().prepare().unwrap();
        original.clone().udf_registry().prepare().unwrap();
        active_snapshot.prepare().unwrap();
        let independent = SimpleOrchestrator::new(
            original.base_directory.clone(),
            config,
            runtime,
            Default::default(),
        );
        assert!(independent.udf_registry().prepare().is_err());
    }
}
