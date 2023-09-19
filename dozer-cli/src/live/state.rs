use std::{sync::Arc, thread::JoinHandle};

use clap::Parser;

use dozer_cache::dozer_log::camino::Utf8Path;
use dozer_core::{app::AppPipeline, dag_schemas::DagSchemas, Dag};
use dozer_sql::builder::statement_to_pipeline;
use dozer_tracing::{Labels, LabelsAndProgress};
use dozer_types::{
    constants::DEFAULT_DEFAULT_MAX_NUM_RECORDS,
    grpc_types::{
        contract::{DotResponse, ProtoResponse, SchemasResponse},
        live::{BuildResponse, BuildStatus, ConnectResponse, LiveApp, LiveResponse, RunRequest},
    },
    log::info,
    models::{
        api_config::{
            default_api_grpc, default_api_rest, default_app_grpc, ApiConfig, AppGrpcOptions,
            GrpcApiOptions, RestApiOptions,
        },
        api_endpoint::ApiEndpoint,
        api_security::ApiSecurity,
        app_config::AppConfig,
        flags::Flags,
    },
};
use tempdir::TempDir;
use tokio::{runtime::Runtime, sync::RwLock};

use crate::{
    cli::{init_dozer, types::Cli},
    errors::OrchestrationError,
    pipeline::PipelineBuilder,
    shutdown::{self, ShutdownReceiver, ShutdownSender},
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

        let dozer = init_dozer(
            runtime,
            cli.config_paths.clone(),
            cli.config_token.clone(),
            cli.config_overrides.clone(),
            cli.ignore_pipe,
            Default::default(),
        )
        .await?;

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
                .endpoints
                .iter()
                .map(|c| c.name.clone())
                .collect();

            let enable_api_security = std::env::var("DOZER_MASTER_SECRET")
                .ok()
                .map(ApiSecurity::Jwt)
                .as_ref()
                .or(dozer
                    .dozer
                    .config
                    .api
                    .as_ref()
                    .and_then(|f| f.api_security.as_ref()))
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
            .map(|schemas| SchemasResponse { schemas })
    }

    pub async fn get_graph_schemas(&self) -> Result<SchemasResponse, LiveError> {
        self.create_contract_if_missing().await?;
        let dozer = self.dozer.read().await;
        let contract = get_contract(&dozer)?;

        Ok(SchemasResponse {
            schemas: contract.get_graph_schemas(),
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

    pub async fn get_protos(&self) -> Result<ProtoResponse, LiveError> {
        let dozer = self.dozer.read().await;
        let contract = get_contract(&dozer)?;
        let (protos, libraries) = contract.get_protos()?;

        Ok(ProtoResponse { protos, libraries })
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
    pub async fn get_api_token(&self, ttl: Option<i32>) -> Result<Option<String>, LiveError> {
        let dozer: tokio::sync::RwLockReadGuard<'_, Option<DozerAndContract>> =
            self.dozer.read().await;
        let dozer = &dozer.as_ref().ok_or(LiveError::NotInitialized)?.dozer;
        let generated_token = dozer.generate_token(ttl).ok();
        Ok(generated_token)
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

async fn create_contract(dozer: SimpleOrchestrator) -> Result<Contract, OrchestrationError> {
    let dag = create_dag(&dozer).await?;
    let version = dozer.config.version;
    let schemas = DagSchemas::new(dag)?;
    let contract = Contract::new(
        version as usize,
        &schemas,
        &dozer.config.connections,
        &dozer.config.endpoints,
        // We don't care about API generation options here. They are handled in `run_all`.
        false,
        true,
    )?;
    Ok(contract)
}

async fn create_dag(dozer: &SimpleOrchestrator) -> Result<Dag, OrchestrationError> {
    let endpoint_and_logs = dozer
        .config
        .endpoints
        .iter()
        // We're not really going to run the pipeline, so we don't create logs.
        .map(|endpoint| (endpoint.clone(), None))
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
    let mut dozer = get_dozer_run_instance(dozer, labels, request, temp_dir)?;

    validate_config(&dozer.config)?;
    let runtime = dozer.runtime.clone();
    let run_thread = std::thread::spawn(move || dozer.run_all(shutdown_receiver, false));

    let handle = std::thread::spawn(move || {
        runtime.block_on(async {
            run_thread.join().unwrap().unwrap();
        });
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
            let context = statement_to_pipeline(
                &req.sql,
                &mut AppPipeline::new(dozer.config.flags.clone().unwrap_or_default().into()),
                None,
                dozer.config.udfs.clone(),
            )
            .map_err(LiveError::PipelineError)?;

            //overwrite sql
            dozer.config.sql = Some(req.sql);

            dozer.config.endpoints = vec![];
            let endpoints = context.output_tables_map.keys().collect::<Vec<_>>();
            for endpoint in endpoints {
                let endpoint = ApiEndpoint {
                    name: endpoint.to_string(),
                    table_name: endpoint.to_string(),
                    path: format!("/{}", endpoint),
                    ..Default::default()
                };
                dozer.config.endpoints.push(endpoint);
            }
        }
        Some(dozer_types::grpc_types::live::run_request::Request::Source(req)) => {
            dozer.config.sql = None;
            dozer.config.endpoints = vec![];
            let endpoint = req.source;
            dozer.config.endpoints.push(ApiEndpoint {
                name: endpoint.to_string(),
                table_name: endpoint.to_string(),
                path: format!("/{}", endpoint),
                ..Default::default()
            });
        }
        None => {}
    };

    if let Some(app) = dozer.config.app.as_mut() {
        app.max_num_records_before_persist = Some(usize::MAX as u64);
        app.max_interval_before_persist_in_seconds = Some(u64::MAX);
    } else {
        dozer.config.app = Some(AppConfig {
            max_num_records_before_persist: Some(usize::MAX as u64),
            max_interval_before_persist_in_seconds: Some(u64::MAX),
            ..Default::default()
        })
    }

    if let Some(api) = dozer.config.api.as_mut() {
        override_api_config(api);
    } else {
        dozer.config.api = Some(ApiConfig {
            api_security: None,
            rest: Some(default_rest_config_for_live()),
            grpc: Some(default_grpc_config_for_live()),
            app_grpc: Some(default_app_grpc_config_for_live()),
            default_max_num_records: DEFAULT_DEFAULT_MAX_NUM_RECORDS as u32,
        })
    }

    dozer.config.home_dir = temp_dir.to_string();
    dozer.config.cache_dir = AsRef::<Utf8Path>::as_ref(temp_dir).join("cache").into();

    dozer.labels = LabelsAndProgress::new(labels, false);

    Ok(dozer)
}

fn override_api_config(api: &mut ApiConfig) {
    if let Some(rest) = api.rest.as_mut() {
        override_rest_config(rest);
    } else {
        api.rest = Some(default_rest_config_for_live());
    }

    if let Some(grpc) = api.grpc.as_mut() {
        override_grpc_config(grpc);
    } else {
        api.grpc = Some(default_grpc_config_for_live());
    }

    if let Some(app_grpc) = api.app_grpc.as_mut() {
        override_app_grpc_config(app_grpc);
    } else {
        api.app_grpc = Some(default_app_grpc_config_for_live());
    }
}

fn override_rest_config(rest: &mut RestApiOptions) {
    rest.host = "0.0.0.0".to_string();
    rest.port = 62996;
    rest.cors = true;
    rest.enabled = true;
}

fn default_rest_config_for_live() -> RestApiOptions {
    let mut rest = default_api_rest();
    override_rest_config(&mut rest);
    rest
}

fn override_grpc_config(grpc: &mut GrpcApiOptions) {
    grpc.host = "0.0.0.0".to_string();
    grpc.port = 62998;
    grpc.cors = true;
    grpc.web = true;
    grpc.enabled = true;
}

fn default_grpc_config_for_live() -> GrpcApiOptions {
    let mut grpc = default_api_grpc();
    override_grpc_config(&mut grpc);
    grpc
}

fn override_app_grpc_config(app_grpc: &mut AppGrpcOptions) {
    app_grpc.port = 62997;
    app_grpc.host = "0.0.0.0".to_string();
}

fn default_app_grpc_config_for_live() -> AppGrpcOptions {
    let mut app_grpc = default_app_grpc();
    override_app_grpc_config(&mut app_grpc);
    app_grpc
}
