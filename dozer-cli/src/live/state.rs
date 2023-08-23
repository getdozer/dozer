use std::{sync::Arc, thread::JoinHandle};

use clap::Parser;
use dozer_core::{app::AppPipeline, dag_schemas::DagSchemas, Dag};
use dozer_sql::pipeline::builder::{statement_to_pipeline, SchemaSQLContext};
use dozer_types::{
    grpc_types::{
        contract::{DotResponse, SchemasResponse},
        live::{ConnectResponse, LiveApp, LiveResponse, RunRequest},
    },
    indicatif::MultiProgress,
    log::info,
    models::{
        api_config::{ApiConfig, AppGrpcOptions},
        api_endpoint::ApiEndpoint,
        telemetry::{TelemetryConfig, TelemetryMetricsConfig},
    },
};
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

pub struct LiveState {
    dozer: RwLock<Option<DozerAndContract>>,
    run_thread: RwLock<Option<ShutdownSender>>,
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
                let dag = create_dag(&dozer_and_contract.dozer).await?;
                let schemas = DagSchemas::new(dag)?;
                let contract = Contract::new(
                    &schemas,
                    &dozer_and_contract.dozer.config.endpoints,
                    // We don't care about API generation options here. They are handled in `run_all`.
                    false,
                    true,
                )?;
                dozer_and_contract.contract = Some(contract);
            }
        }
        Ok(())
    }

    pub async fn set_sender(&self, sender: tokio::sync::broadcast::Sender<ConnectResponse>) {
        *self.sender.write().await = Some(sender);
    }

    pub async fn broadcast(&self) {
        let sender = self.sender.read().await;
        info!("broadcasting current state");
        if let Some(sender) = sender.as_ref() {
            let res = self.get_current().await;
            // Ignore broadcast error.
            let _ = sender.send(ConnectResponse {
                live: Some(res),
                progress: None,
            });
        }
    }

    pub async fn set_dozer(&self, dozer: Option<SimpleOrchestrator>) {
        *self.dozer.write().await = dozer.map(|dozer| DozerAndContract {
            dozer,
            contract: None,
        });
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
            false,
        )
        .await?;

        *lock = Some(DozerAndContract {
            dozer,
            contract: None,
        });
        Ok(())
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
            LiveApp {
                app_name: dozer.dozer.config.app_name.clone(),
                connections,
                endpoints,
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

    pub async fn run(&self, request: RunRequest) -> Result<(), LiveError> {
        let dozer = self.dozer.read().await;
        let dozer = &dozer.as_ref().ok_or(LiveError::NotInitialized)?.dozer;

        // kill if a handle already exists
        self.stop().await?;

        let (shutdown_sender, shutdown_receiver) = shutdown::new(&dozer.runtime);
        let metrics_shutdown = shutdown_receiver.clone();
        let _handle = run(dozer.clone(), request, shutdown_receiver)?;

        // Initialize progress
        let metrics_sender = self.sender.read().await.as_ref().unwrap().clone();
        tokio::spawn(async {
            progress_stream(metrics_sender, metrics_shutdown)
                .await
                .unwrap()
        });

        let mut lock = self.run_thread.write().await;
        *lock = Some(shutdown_sender);

        Ok(())
    }

    pub async fn stop(&self) -> Result<(), LiveError> {
        let mut lock = self.run_thread.write().await;
        if let Some(shutdown) = lock.take() {
            shutdown.shutdown()
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

async fn create_dag(
    dozer: &SimpleOrchestrator,
) -> Result<Dag<SchemaSQLContext>, OrchestrationError> {
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
        MultiProgress::new(),
    );
    let (_shutdown_sender, shutdown_receiver) = shutdown::new(&dozer.runtime);
    builder.build(&dozer.runtime, shutdown_receiver).await
}

fn run(
    dozer: SimpleOrchestrator,
    request: RunRequest,
    shutdown_receiver: ShutdownReceiver,
) -> Result<JoinHandle<()>, OrchestrationError> {
    let mut dozer = get_dozer_run_instance(dozer, request)?;

    validate_config(&dozer.config)?;

    let runtime = dozer.runtime.clone();
    let run_thread = std::thread::spawn(move || {
        dozer.build(true, shutdown_receiver.clone()).unwrap();
        dozer.run_all(shutdown_receiver)
    });

    let handle = std::thread::spawn(move || {
        runtime.block_on(async {
            run_thread.join().unwrap().unwrap();
        });
    });

    Ok(handle)
}

fn get_dozer_run_instance(
    mut dozer: SimpleOrchestrator,
    req: RunRequest,
) -> Result<SimpleOrchestrator, LiveError> {
    match req.request {
        Some(dozer_types::grpc_types::live::run_request::Request::Sql(req)) => {
            let context = statement_to_pipeline(&req.sql, &mut AppPipeline::new(), None)
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

    dozer.config.api = Some(ApiConfig {
        app_grpc: Some(AppGrpcOptions {
            port: 5678,
            host: "0.0.0.0".to_string(),
        }),

        ..Default::default()
    });

    dozer.config.home_dir = tempdir::TempDir::new("live")
        .unwrap()
        .into_path()
        .to_string_lossy()
        .to_string();

    dozer.config.telemetry = Some(TelemetryConfig {
        trace: None,
        metrics: Some(TelemetryMetricsConfig::Prometheus(())),
    });

    Ok(dozer)
}
