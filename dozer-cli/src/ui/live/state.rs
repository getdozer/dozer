use std::{collections::HashMap, sync::Arc, thread::JoinHandle};

use clap::Parser;

use dozer_cache::dozer_log::camino::Utf8Path;
use dozer_core::shutdown::{self, ShutdownReceiver, ShutdownSender};
use dozer_core::{dag_schemas::DagSchemas, Dag};
use dozer_tracing::{Labels, LabelsAndProgress};
use dozer_types::{
    grpc_types::{
        contract::DotResponse,
        live::{BuildResponse, BuildStatus, ConnectResponse, LiveApp, LiveResponse, RunRequest},
        types::SchemasResponse,
    },
    log::info,
    models::{
        api_config::{ApiConfig, AppGrpcOptions, GrpcApiOptions, RestApiOptions},
        api_security::ApiSecurity,
        flags::Flags,
    },
};
use tempdir::TempDir;
use tokio::{runtime::Runtime, sync::RwLock};

use crate::{
    cli::{init_config, init_dozer, types::Cli},
    errors::OrchestrationError,
    pipeline::{EndpointLog, EndpointLogKind, PipelineBuilder},
    simple::{helper::validate_config, Contract, SimpleOrchestrator},
};

use super::{progress::progress_stream, LiveError};

struct DozerAndContract {
    dozer: SimpleOrchestrator,
    contract: Option<Contract>,
}

pub struct ShutdownAndTempDir {
    shutdown: ShutdownSender,
    _temp_dir: TempDir,
}

#[derive(Debug)]
pub enum BroadcastType {
    Start,
    Success,
    Failed(String),
}

pub struct LiveState {
    dozer: RwLock<Option<DozerAndContract>>,
    run_thread: RwLock<Option<ShutdownAndTempDir>>,
    error_message: RwLock<Option<String>>,
    sender: RwLock<Option<tokio::sync::broadcast::Sender<ConnectResponse>>>,
}

impl Default for LiveState {
    fn default() -> Self {
        Self::new()
    }
}
impl LiveState {
    pub fn new() -> Self {
        Self {
            dozer: RwLock::new(None),
            run_thread: RwLock::new(None),
            sender: RwLock::new(None),
            error_message: RwLock::new(None),
        }
    }

    async fn create_contract_if_missing(&self) -> Result<(), LiveError> {
        let mut dozer_and_contract_lock = self.dozer.write().await;
        if let Some(dozer_and_contract) = dozer_and_contract_lock.as_mut() {
            if dozer_and_contract.contract.is_none() {
                let contract = create_contract(dozer_and_contract.dozer.clone()).await?;
                dozer_and_contract.contract = Some(contract);
            }
        }
        Ok(())
    }

    pub async fn set_sender(&self, sender: tokio::sync::broadcast::Sender<ConnectResponse>) {
        *self.sender.write().await = Some(sender);
    }

    pub async fn broadcast(&self, broadcast_type: BroadcastType) {
        let sender = self.sender.read().await;
        info!("Broadcasting state: {:?}", broadcast_type);
        if let Some(sender) = sender.as_ref() {
            let res = match broadcast_type {
                BroadcastType::Start => ConnectResponse {
                    live: None,
                    progress: None,
                    build: Some(BuildResponse {
                        status: BuildStatus::BuildStart as i32,
                        message: None,
                    }),
                },
                BroadcastType::Failed(msg) => ConnectResponse {
                    live: None,
                    progress: None,
                    build: Some(BuildResponse {
                        status: BuildStatus::BuildFailed as i32,
                        message: Some(msg),
                    }),
                },
                BroadcastType::Success => {
                    let res = self.get_current().await;
                    ConnectResponse {
                        live: Some(res),
                        progress: None,
                        build: None,
                    }
                }
            };
            let _ = sender.send(res);
        }
    }

    pub async fn set_error_message(&self, error_message: Option<String>) {
        *self.error_message.write().await = error_message;
    }

    pub async fn build(&self, runtime: Arc<Runtime>) -> Result<(), LiveError> {
        // Taking lock to ensure that we don't have multiple builds running at the same time
        let mut lock = self.dozer.write().await;

        let cli = Cli::parse();

        let (config, _) = init_config(
            cli.config_paths.clone(),
            cli.config_token.clone(),
            cli.config_overrides.clone(),
            cli.ignore_pipe,
        )
        .await?;
        let dozer = init_dozer(runtime, config, Default::default())?;

        let contract = create_contract(dozer.clone()).await;
        *lock = Some(DozerAndContract {
            dozer,
            contract: match &contract {
                Ok(contract) => Some(contract.clone()),
                Err(_) => None,
            },
        });
        if let Err(e) = &contract {
            self.set_error_message(Some(e.to_string())).await;
        } else {
            self.set_error_message(None).await;
        }

        contract
            .map(|_| ())
            .map_err(|e| LiveError::OrchestrationError(Box::new(e)))
    }
    pub async fn get_current(&self) -> LiveResponse {
        let dozer = self.dozer.read().await;
        let app = dozer.as_ref().map(|dozer| {
            let connections = dozer
                .dozer
                .config
                .connections
                .iter()
                .map(|c| c.name.clone())
                .collect();
            let endpoints = dozer
                .dozer
                .config
                .sinks
                .iter()
                .map(|endpoint| endpoint.table_name.clone())
                .collect();

            let enable_api_security = std::env::var("DOZER_MASTER_SECRET")
                .ok()
                .map(ApiSecurity::Jwt)
                .as_ref()
                .or(dozer.dozer.config.api.api_security.as_ref())
                .is_some();
            LiveApp {
                app_name: dozer.dozer.config.app_name.clone(),
                connections,
                endpoints,
                enable_api_security,
            }
        });

        LiveResponse {
            initialized: app.is_some(),
            running: self.run_thread.read().await.is_some(),
            error_message: self.error_message.read().await.as_ref().cloned(),
            app,
        }
    }

    pub async fn get_endpoints_schemas(&self) -> Result<SchemasResponse, LiveError> {
        self.create_contract_if_missing().await?;
        let dozer = self.dozer.read().await;
        let contract = get_contract(&dozer)?;
        Ok(SchemasResponse {
            schemas: contract.get_endpoints_schemas(),
            errors: HashMap::new(),
        })
    }
    pub async fn get_source_schemas(
        &self,
        connection_name: String,
    ) -> Result<SchemasResponse, LiveError> {
        self.create_contract_if_missing().await?;
        let dozer = self.dozer.read().await;
        let contract = get_contract(&dozer)?;

        contract
            .get_source_schemas(&connection_name)
            .ok_or(LiveError::ConnectionNotFound(connection_name))
            .map(|schemas| SchemasResponse {
                schemas,
                errors: HashMap::new(),
            })
    }

    pub async fn get_graph_schemas(&self) -> Result<SchemasResponse, LiveError> {
        self.create_contract_if_missing().await?;
        let dozer = self.dozer.read().await;
        let contract = get_contract(&dozer)?;

        Ok(SchemasResponse {
            schemas: contract.get_graph_schemas(),
            errors: HashMap::new(),
        })
    }

    pub async fn generate_dot(&self) -> Result<DotResponse, LiveError> {
        self.create_contract_if_missing().await?;
        let dozer = self.dozer.read().await;
        let contract = get_contract(&dozer)?;

        Ok(DotResponse {
            dot: contract.generate_dot(),
        })
    }

    pub async fn run(&self, request: RunRequest) -> Result<Labels, LiveError> {
        let dozer = self.dozer.read().await;
        let dozer = &dozer.as_ref().ok_or(LiveError::NotInitialized)?.dozer;
        // kill if a handle already exists
        self.stop().await?;
        let temp_dir = TempDir::new("live")?;
        let temp_dir_path = temp_dir.path().to_str().unwrap();

        let labels: Labels = [("live_run_id", uuid::Uuid::new_v4().to_string())]
            .into_iter()
            .collect();
        let (shutdown_sender, shutdown_receiver) = shutdown::new(&dozer.runtime);
        let metrics_shutdown = shutdown_receiver.clone();
        let _handle = run(
            dozer.clone(),
            labels.clone(),
            request,
            shutdown_receiver,
            temp_dir_path,
        )?;

        // Initialize progress
        let metrics_sender = self.sender.read().await.as_ref().unwrap().clone();
        let labels_clone = labels.clone();
        tokio::spawn(async {
            progress_stream(metrics_sender, metrics_shutdown, labels_clone)
                .await
                .unwrap()
        });
        let mut lock = self.run_thread.write().await;
        if let Some(shutdown_and_tempdir) = lock.take() {
            shutdown_and_tempdir.shutdown.shutdown();
        }
        let shutdown_and_tempdir = ShutdownAndTempDir {
            shutdown: shutdown_sender,
            _temp_dir: temp_dir,
        };
        *lock = Some(shutdown_and_tempdir);
        Ok(labels)
    }

    pub async fn stop(&self) -> Result<(), LiveError> {
        let mut lock = self.run_thread.write().await;
        if let Some(shutdown_and_tempdir) = lock.take() {
            shutdown_and_tempdir.shutdown.shutdown();
            shutdown_and_tempdir._temp_dir.close()?;
        }
        *lock = None;
        Ok(())
    }
}

fn get_contract(dozer_and_contract: &Option<DozerAndContract>) -> Result<&Contract, LiveError> {
    dozer_and_contract
        .as_ref()
        .ok_or(LiveError::NotInitialized)?
        .contract
        .as_ref()
        .ok_or(LiveError::NotInitialized)
}

pub async fn create_contract(dozer: SimpleOrchestrator) -> Result<Contract, OrchestrationError> {
    let dag = create_dag(&dozer).await?;
    let version = dozer.config.version;
    let schemas = DagSchemas::new(dag).await?;
    let contract = Contract::new(
        version as usize,
        &schemas,
        &dozer.config.connections,
        &dozer.config.sinks,
        // We don't care about API generation options here. They are handled in `run_all`.
        false,
        true,
    )?;
    Ok(contract)
}

pub async fn create_dag(dozer: &SimpleOrchestrator) -> Result<Dag, OrchestrationError> {
    let endpoint_and_logs = dozer
        .config
        .sinks
        .iter()
        // We're not really going to run the pipeline, so we don't create logs.
        .map(|endpoint| EndpointLog {
            table_name: endpoint.table_name.clone(),
            kind: EndpointLogKind::Dummy,
        })
        .collect();
    let builder = PipelineBuilder::new(
        &dozer.config.connections,
        &dozer.config.sources,
        dozer.config.sql.as_deref(),
        endpoint_and_logs,
        Default::default(),
        Flags::default(),
        &dozer.config.udfs,
    );
    let (_shutdown_sender, shutdown_receiver) = shutdown::new(&dozer.runtime);
    builder.build(&dozer.runtime, shutdown_receiver).await
}

fn run(
    dozer: SimpleOrchestrator,
    labels: Labels,
    request: RunRequest,
    shutdown_receiver: ShutdownReceiver,
    temp_dir: &str,
) -> Result<JoinHandle<()>, OrchestrationError> {
    let dozer = get_dozer_run_instance(dozer, labels, request, temp_dir)?;

    validate_config(&dozer.config)?;
    let runtime = dozer.runtime.clone();

    let handle = std::thread::spawn(move || {
        runtime.block_on(async move { dozer.run_all(shutdown_receiver, false).await.unwrap() });
    });

    Ok(handle)
}

fn get_dozer_run_instance(
    mut dozer: SimpleOrchestrator,
    labels: Labels,
    req: RunRequest,
    temp_dir: &str,
) -> Result<SimpleOrchestrator, LiveError> {
    match req.request {
        Some(dozer_types::grpc_types::live::run_request::Request::Sql(req)) => {
            //overwrite sql
            dozer.config.sql = Some(req.sql);
            dozer.config.sinks = vec![];
        }
        Some(dozer_types::grpc_types::live::run_request::Request::Source(_req)) => {
            dozer.config.sql = None;
            dozer.config.sinks = vec![];
        }
        None => {}
    };

    override_api_config(&mut dozer.config.api);

    dozer.config.flags.enable_app_checkpoints = Some(false);

    dozer.config.home_dir = Some(temp_dir.to_string());
    dozer.config.cache_dir = Some(AsRef::<Utf8Path>::as_ref(temp_dir).join("cache").into());

    dozer.labels = LabelsAndProgress::new(labels, false);

    Ok(dozer)
}

fn override_api_config(api: &mut ApiConfig) {
    override_rest_config(&mut api.rest);
    override_grpc_config(&mut api.grpc);
    override_app_grpc_config(&mut api.app_grpc);
}

fn override_rest_config(rest: &mut RestApiOptions) {
    rest.host = Some("0.0.0.0".to_string());
    rest.port = Some(62996);
    rest.cors = Some(true);
    rest.enabled = Some(true);
}

fn override_grpc_config(grpc: &mut GrpcApiOptions) {
    grpc.host = Some("0.0.0.0".to_string());
    grpc.port = Some(62998);
    grpc.cors = Some(true);
    grpc.web = Some(true);
    grpc.enabled = Some(true);
}

fn override_app_grpc_config(app_grpc: &mut AppGrpcOptions) {
    app_grpc.port = Some(62997);
    app_grpc.host = Some("0.0.0.0".to_string());
}
