use std::{collections::HashMap, sync::Arc, thread::JoinHandle};

use clap::Parser;

use dozer_core::shutdown::{self, ShutdownReceiver, ShutdownSender};
use dozer_core::{dag_schemas::DagSchemas, Dag};
use dozer_tracing::LabelsAndProgress;
use dozer_types::{
    grpc_types::{
        app_ui::{AppUi, AppUiResponse, BuildResponse, BuildStatus, ConnectResponse, RunRequest},
        contract::DotResponse,
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

use super::AppUIError;
use crate::{
    cli::{init_config, init_dozer, types::Cli},
    errors::OrchestrationError,
    pipeline::PipelineBuilder,
    simple::{helper::validate_config, Contract, SimpleOrchestrator},
};
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
pub struct AppUIState {
    dozer: RwLock<Option<DozerAndContract>>,
    run_thread: RwLock<Option<ShutdownAndTempDir>>,
    error_message: RwLock<Option<String>>,
    sender: RwLock<Option<tokio::sync::broadcast::Sender<ConnectResponse>>>,
}

impl Default for AppUIState {
    fn default() -> Self {
        Self::new()
    }
}
impl AppUIState {
    pub fn new() -> Self {
        Self {
            dozer: RwLock::new(None),
            run_thread: RwLock::new(None),
            sender: RwLock::new(None),
            error_message: RwLock::new(None),
        }
    }

    async fn create_contract_if_missing(&self) -> Result<(), AppUIError> {
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
                    app_ui: None,
                    build: Some(BuildResponse {
                        status: BuildStatus::BuildStart as i32,
                        message: None,
                    }),
                },
                BroadcastType::Failed(msg) => ConnectResponse {
                    app_ui: None,
                    build: Some(BuildResponse {
                        status: BuildStatus::BuildFailed as i32,
                        message: Some(msg),
                    }),
                },
                BroadcastType::Success => {
                    let res = self.get_current().await;
                    ConnectResponse {
                        app_ui: Some(res),
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

    pub async fn build(&self, runtime: Arc<Runtime>) -> Result<(), AppUIError> {
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
            .map_err(|e| AppUIError::OrchestrationError(Box::new(e)))
    }
    pub async fn get_current(&self) -> AppUiResponse {
        let dozer = self.dozer.read().await;
        let app = dozer.as_ref().map(|dozer| {
            let config = &dozer.dozer.config;
            let connections_in_source: Vec<String> = config
                .sources
                .iter()
                .map(|source| source.connection.clone())
                .collect::<std::collections::HashSet<String>>()
                .into_iter()
                .collect();
            let sink_names: Vec<String> =
                config.sinks.iter().map(|sink| sink.name.clone()).collect();

            let enable_api_security = std::env::var("DOZER_MASTER_SECRET")
                .ok()
                .map(ApiSecurity::Jwt)
                .as_ref()
                .or(dozer.dozer.config.api.api_security.as_ref())
                .is_some();
            AppUi {
                app_name: dozer.dozer.config.app_name.clone(),
                connections: connections_in_source,
                sink_names,
                enable_api_security,
            }
        });
        AppUiResponse {
            initialized: app.is_some(),
            running: self.run_thread.read().await.is_some(),
            error_message: self.error_message.read().await.as_ref().cloned(),
            app,
        }
    }

    pub async fn get_sink_table_schemas(
        &self,
        sink_name: String,
    ) -> Result<SchemasResponse, AppUIError> {
        self.create_contract_if_missing().await?;
        let dozer = self.dozer.read().await;
        let contract = get_contract(&dozer)?;

        contract
            .get_sink_table_schemas(&sink_name)
            .ok_or(AppUIError::SinkNotFound(sink_name))
            .map(|schemas| SchemasResponse {
                schemas,
                errors: HashMap::new(),
            })
    }
    pub async fn get_source_schemas(
        &self,
        connection_name: String,
    ) -> Result<SchemasResponse, AppUIError> {
        self.create_contract_if_missing().await?;
        let dozer = self.dozer.read().await;
        let contract = get_contract(&dozer)?;

        contract
            .get_source_schemas(&connection_name)
            .ok_or(AppUIError::ConnectionNotFound(connection_name))
            .map(|schemas| SchemasResponse {
                schemas,
                errors: HashMap::new(),
            })
    }

    pub async fn get_graph_schemas(&self) -> Result<SchemasResponse, AppUIError> {
        self.create_contract_if_missing().await?;
        let dozer = self.dozer.read().await;
        let contract = get_contract(&dozer)?;

        Ok(SchemasResponse {
            schemas: contract.get_graph_schemas(),
            errors: HashMap::new(),
        })
    }

    pub async fn generate_dot(&self) -> Result<DotResponse, AppUIError> {
        self.create_contract_if_missing().await?;
        let dozer = self.dozer.read().await;
        let contract = get_contract(&dozer)?;

        Ok(DotResponse {
            dot: contract.generate_dot(),
        })
    }

    pub async fn run(&self, request: RunRequest) -> Result<String, AppUIError> {
        let dozer = self.dozer.read().await;
        let dozer = &dozer.as_ref().ok_or(AppUIError::NotInitialized)?.dozer;
        // kill if a handle already exists
        self.stop().await?;
        let temp_dir = TempDir::new("dozer_app_local")?;
        let temp_dir_path = temp_dir.path().to_str().unwrap();

        let application_id = uuid::Uuid::new_v4().to_string();
        let (shutdown_sender, shutdown_receiver) = shutdown::new(&dozer.runtime);
        let _handle = run(
            dozer.clone(),
            application_id.clone(),
            request,
            shutdown_receiver,
            temp_dir_path,
        )?;

        let mut lock = self.run_thread.write().await;
        if let Some(shutdown_and_tempdir) = lock.take() {
            shutdown_and_tempdir.shutdown.shutdown();
        }
        let shutdown_and_tempdir = ShutdownAndTempDir {
            shutdown: shutdown_sender,
            _temp_dir: temp_dir,
        };
        *lock = Some(shutdown_and_tempdir);
        Ok(application_id)
    }

    pub async fn stop(&self) -> Result<(), AppUIError> {
        let mut lock = self.run_thread.write().await;
        if let Some(shutdown_and_tempdir) = lock.take() {
            shutdown_and_tempdir.shutdown.shutdown();
            shutdown_and_tempdir._temp_dir.close()?;
        }
        *lock = None;
        Ok(())
    }
}

fn get_contract(dozer_and_contract: &Option<DozerAndContract>) -> Result<&Contract, AppUIError> {
    dozer_and_contract
        .as_ref()
        .ok_or(AppUIError::NotInitialized)?
        .contract
        .as_ref()
        .ok_or(AppUIError::NotInitialized)
}

pub async fn create_contract(dozer: SimpleOrchestrator) -> Result<Contract, OrchestrationError> {
    let dag = create_dag(&dozer).await?;
    let version = dozer.config.version;
    let schemas = DagSchemas::new(dag).await?;
    let contract = Contract::new(version as usize, &schemas, &dozer.config.connections)?;
    Ok(contract)
}

pub async fn create_dag(dozer: &SimpleOrchestrator) -> Result<Dag, OrchestrationError> {
    let builder = PipelineBuilder::new(
        &dozer.config.connections,
        &dozer.config.sources,
        dozer.config.sql.as_deref(),
        &dozer.config.sinks,
        Default::default(),
        Flags::default(),
        &dozer.config.udfs,
    );
    let (_shutdown_sender, shutdown_receiver) = shutdown::new(&dozer.runtime);
    builder.build(&dozer.runtime, shutdown_receiver).await
}

fn run(
    dozer: SimpleOrchestrator,
    application_id: String,
    request: RunRequest,
    shutdown_receiver: ShutdownReceiver,
    temp_dir: &str,
) -> Result<JoinHandle<()>, OrchestrationError> {
    let dozer = get_dozer_run_instance(dozer, application_id, request, temp_dir)?;

    validate_config(&dozer.config)?;
    let runtime = dozer.runtime.clone();

    let handle: JoinHandle<()> = std::thread::spawn(move || {
        runtime.block_on(async move { dozer.run_all(shutdown_receiver, false).await.unwrap() });
    });

    Ok(handle)
}

fn get_dozer_run_instance(
    mut dozer: SimpleOrchestrator,
    application_id: String,
    req: RunRequest,
    temp_dir: &str,
) -> Result<SimpleOrchestrator, AppUIError> {
    match req.request {
        Some(dozer_types::grpc_types::app_ui::run_request::Request::Sql(req)) => {
            //overwrite sql
            dozer.config.sql = Some(req.sql);
            dozer.config.sinks = vec![];
        }
        Some(dozer_types::grpc_types::app_ui::run_request::Request::Source(_req)) => {
            dozer.config.sql = None;
            dozer.config.sinks = vec![];
        }
        None => {}
    };

    override_api_config(&mut dozer.config.api);

    dozer.config.flags.enable_app_checkpoints = Some(false);

    dozer.config.home_dir = Some(temp_dir.to_string());

    dozer.labels = LabelsAndProgress::new(application_id, false);

    Ok(dozer)
}

fn override_api_config(api: &mut ApiConfig) {
    override_rest_config(&mut api.rest);
    override_grpc_config(&mut api.grpc);
    override_app_grpc_config(&mut api.app_grpc);
    api.pgwire.enabled = Some(true);
}

fn override_rest_config(rest: &mut RestApiOptions) {
    rest.host = Some("0.0.0.0".to_string());
    rest.port = Some(62885);
    rest.cors = Some(true);
    rest.enabled = Some(true);
    rest.enable_sql = Some(true);
}

fn override_grpc_config(grpc: &mut GrpcApiOptions) {
    grpc.host = Some("0.0.0.0".to_string());
    grpc.port = Some(62887);
    grpc.cors = Some(true);
    grpc.web = Some(true);
    grpc.enabled = Some(true);
}

fn override_app_grpc_config(app_grpc: &mut AppGrpcOptions) {
    app_grpc.port = Some(62997);
    app_grpc.host = Some("0.0.0.0".to_string());
}
