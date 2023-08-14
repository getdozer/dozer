use std::thread::JoinHandle;

use clap::Parser;
use dozer_core::{app::AppPipeline, dag_schemas::DagSchemas, petgraph::dot, Dag};
use dozer_ingestion::connectors::get_connector;
use dozer_sql::pipeline::builder::{statement_to_pipeline, SchemaSQLContext};
use dozer_types::{
    grpc_types::live::{
        ConnectResponse, DotResponse, LiveApp, LiveResponse, RunRequest, SchemasResponse,
        SqlResponse,
    },
    indicatif::MultiProgress,
    log::info,
    models::{
        api_config::{ApiConfig, AppGrpcOptions},
        api_endpoint::ApiEndpoint,
        telemetry::{TelemetryConfig, TelemetryMetricsConfig},
    },
    parking_lot::RwLock,
};

use crate::{
    cli::{init_dozer, types::Cli},
    errors::OrchestrationError,
    pipeline::PipelineBuilder,
    shutdown::{self, ShutdownReceiver, ShutdownSender},
    simple::{helper::validate_config, SimpleOrchestrator},
};

use super::{
    graph::{map_dag_schemas, transform_dag_ui},
    helper::map_schema,
    progress::progress_stream,
    LiveError,
};

pub struct LiveState {
    dozer: RwLock<Option<SimpleOrchestrator>>,
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

    pub fn get_dozer(&self) -> Option<SimpleOrchestrator> {
        self.dozer.read().as_ref().map(|d| d.clone())
    }

    pub fn set_sender(&self, sender: tokio::sync::broadcast::Sender<ConnectResponse>) {
        let mut lock = self.sender.write();
        *lock = Some(sender);
    }

    pub fn broadcast(&self) -> Result<(), LiveError> {
        let sender = self.sender.read();
        info!("broadcasting current state");
        if let Some(sender) = sender.as_ref() {
            let res = self.get_current();
            return match sender.send(ConnectResponse {
                live: Some(res),
                progress: None,
            }) {
                Ok(_) => Ok(()),
                Err(e) => Err(LiveError::SendError(e)),
            };
        }

        Ok(())
    }

    pub fn set_dozer(&self, dozer: Option<SimpleOrchestrator>) {
        let mut lock = self.dozer.write();
        *lock = dozer;
    }

    pub fn set_error_message(&self, error_message: Option<String>) {
        let mut lock = self.error_message.write();
        *lock = error_message;
    }

    pub fn build(&self) -> Result<(), LiveError> {
        // Taking lock to ensure that we don't have multiple builds running at the same time
        let mut lock = self.dozer.write();

        let cli = Cli::parse();

        let res = init_dozer(
            cli.config_paths.clone(),
            cli.config_token.clone(),
            cli.config_overrides.clone(),
            None,
        )?;

        *lock = Some(res);
        Ok(())
    }
    pub fn get_current(&self) -> LiveResponse {
        let app = self.get_dozer().map(|dozer| {
            let connections = dozer
                .config
                .connections
                .into_iter()
                .map(|c| c.name)
                .collect();
            let endpoints = dozer.config.endpoints.into_iter().map(|c| c.name).collect();
            LiveApp {
                app_name: dozer.config.app_name,
                connections,
                endpoints,
            }
        });

        LiveResponse {
            initialized: app.is_some(),
            running: self.run_thread.read().is_some(),
            error_message: self.error_message.read().as_ref().map(|e| e.clone()),
            app,
        }
    }

    pub fn get_sql(&self) -> Result<SqlResponse, LiveError> {
        let dozer = self
            .get_dozer()
            .map_or(Err(LiveError::NotInitialized), Ok)?;
        let sql = dozer.config.sql.clone().unwrap_or_default();
        Ok(SqlResponse { sql })
    }
    pub fn get_endpoints_schemas(&self) -> Result<SchemasResponse, LiveError> {
        let dozer = self
            .get_dozer()
            .map_or(Err(LiveError::NotInitialized), Ok)?;
        get_endpoint_schemas(dozer).map_err(|e| LiveError::BoxedError(Box::new(e)))
    }
    pub async fn get_source_schemas(
        &self,
        connection_name: String,
    ) -> Result<SchemasResponse, LiveError> {
        let dozer = self
            .get_dozer()
            .map_or(Err(LiveError::NotInitialized), Ok)?;
        get_source_schemas(dozer, connection_name)
            .await
            .map_err(|e| LiveError::BoxedError(Box::new(e)))
    }

    pub fn get_graph_schemas(&self) -> Result<SchemasResponse, LiveError> {
        let dozer = self
            .get_dozer()
            .map_or(Err(LiveError::NotInitialized), Ok)?;
        get_graph_schemas(dozer).map_err(|e| LiveError::BoxedError(Box::new(e)))
    }

    pub fn generate_dot(&self) -> Result<DotResponse, LiveError> {
        let dozer = self
            .get_dozer()
            .map_or(Err(LiveError::NotInitialized), Ok)?;
        generate_dot(dozer).map_err(|e| LiveError::BoxedError(Box::new(e)))
    }

    pub fn run(&self, request: RunRequest) -> Result<(), LiveError> {
        let dozer = self
            .get_dozer()
            .map_or(Err(LiveError::NotInitialized), Ok)?;

        // kill if a handle already exists
        self.stop()?;

        let (shutdown_sender, shutdown_receiver) = shutdown::new(&dozer.runtime);
        let metrics_shutdown = shutdown_receiver.clone();
        let _handle = run(dozer, request, shutdown_receiver)
            .map_err(|e| LiveError::BoxedError(Box::new(e)))?;

        // Initialize progress
        let metrics_sender = self.sender.read().as_ref().unwrap().clone();
        tokio::spawn(async {
            progress_stream(metrics_sender, metrics_shutdown)
                .await
                .unwrap()
        });

        let mut lock = self.run_thread.write();
        *lock = Some(shutdown_sender);

        Ok(())
    }

    pub fn stop(&self) -> Result<(), LiveError> {
        let mut lock = self.run_thread.write();
        if let Some(shutdown) = lock.take() {
            shutdown.shutdown()
        }
        *lock = None;
        Ok(())
    }
}

pub async fn get_source_schemas(
    dozer: SimpleOrchestrator,
    connection_name: String,
) -> Result<SchemasResponse, OrchestrationError> {
    let connection = dozer
        .config
        .connections
        .iter()
        .find(|c| c.name == connection_name)
        .unwrap();

    let connector =
        get_connector(connection.clone()).map_err(|e| LiveError::BoxedError(Box::new(e)))?;

    let (tables, schemas) = connector
        .list_all_schemas()
        .await
        .map_err(|e| LiveError::BoxedError(Box::new(e)))?;

    let schemas = schemas
        .into_iter()
        .zip(tables)
        .map(|(s, table)| {
            let schema = s.schema;
            (table.name, map_schema(schema))
        })
        .collect();

    Ok(SchemasResponse { schemas })
}

pub fn get_dag(dozer: SimpleOrchestrator) -> Result<Dag<SchemaSQLContext>, OrchestrationError> {
    // Calculate schemas.
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
    builder.build(dozer.runtime.clone())
}

pub fn get_endpoint_schemas(
    dozer: SimpleOrchestrator,
) -> Result<SchemasResponse, OrchestrationError> {
    let dag = get_dag(dozer)?;
    let dag_schemas = DagSchemas::new(dag)?;

    let schemas = dag_schemas.get_sink_schemas();

    let schemas = schemas
        .into_iter()
        .map(|(name, tuple)| {
            let (schema, _) = tuple;
            (name, map_schema(schema))
        })
        .collect();
    Ok(SchemasResponse { schemas })
}

pub fn generate_dot(dozer: SimpleOrchestrator) -> Result<DotResponse, OrchestrationError> {
    let dag = get_dag(dozer)?;
    let dag = transform_dag_ui(&dag);
    let dot_str = dot::Dot::new(dag.graph()).to_string();
    Ok(DotResponse { dot: dot_str })
}

pub fn get_graph_schemas(dozer: SimpleOrchestrator) -> Result<SchemasResponse, OrchestrationError> {
    let dag = get_dag(dozer)?;
    let dag_schemas = DagSchemas::new(dag)?;

    Ok(SchemasResponse {
        schemas: map_dag_schemas(dag_schemas),
    })
}

pub fn run(
    dozer: SimpleOrchestrator,
    request: RunRequest,
    shutdown_receiver: ShutdownReceiver,
) -> Result<JoinHandle<()>, OrchestrationError> {
    let mut dozer = get_dozer_run_instance(dozer, request)?;

    validate_config(&dozer.config).map_err(|e| LiveError::BoxedError(Box::new(e)))?;

    let runtime = dozer.runtime.clone();
    let run_thread = std::thread::spawn(move || {
        dozer.build(true).unwrap();
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
    dozer: SimpleOrchestrator,
    req: RunRequest,
) -> Result<SimpleOrchestrator, LiveError> {
    let mut dozer = dozer;

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
