use std::thread::JoinHandle;

use clap::Parser;
use dozer_api::grpc::types_helper::map_field_definitions;
use dozer_cache::dozer_log::{
    reader::{LogReaderBuilder, LogReaderOptions},
    replication::LogOperation,
};
use dozer_core::{app::AppPipeline, dag_schemas::DagSchemas, petgraph::dot};
use dozer_ingestion::connectors::get_connector;
use dozer_sql::pipeline::builder::statement_to_pipeline;
use dozer_types::{
    grpc_types::{
        live::{DotResponse, LiveApp, LiveResponse, Schema, SchemasResponse, SqlResponse},
        types::Operation,
    },
    indicatif::MultiProgress,
    log::info,
    models::{
        api_config::{ApiConfig, AppGrpcOptions},
        api_endpoint::ApiEndpoint,
    },
    parking_lot::RwLock,
};

use crate::{
    cli::{init_dozer, types::Cli},
    errors::OrchestrationError,
    live::helper::map_operation,
    pipeline::PipelineBuilder,
    shutdown::{self, ShutdownReceiver, ShutdownSender},
    simple::SimpleOrchestrator,
    utils::get_app_grpc_config,
};

use super::LiveError;

pub struct LiveState {
    dozer: RwLock<Option<SimpleOrchestrator>>,
    sql_thread: RwLock<Option<ShutdownSender>>,
}

impl LiveState {
    pub fn new() -> Self {
        Self {
            dozer: RwLock::new(None),
            sql_thread: RwLock::new(None),
        }
    }

    pub fn get_dozer(&self) -> Result<SimpleOrchestrator, LiveError> {
        match self.dozer.read().as_ref() {
            Some(dozer) => Ok(dozer.clone()),
            None => Err(LiveError::NotInitialized),
        }
    }

    pub fn set_dozer(&self, dozer: SimpleOrchestrator) {
        let mut lock = self.dozer.write();
        *lock = Some(dozer);
    }

    pub fn build(&self) -> Result<(), LiveError> {
        let cli = Cli::parse();

        let res = init_dozer(
            cli.config_paths.clone(),
            cli.config_token.clone(),
            cli.config_overrides.clone(),
        )?;

        self.set_dozer(res);
        Ok(())
    }
    pub fn get_current(&self) -> Result<LiveResponse, LiveError> {
        let dozer = self.get_dozer();
        match dozer {
            Ok(dozer) => {
                let connections = dozer
                    .config
                    .connections
                    .into_iter()
                    .map(|c| c.name)
                    .collect();
                let endpoints = dozer.config.endpoints.into_iter().map(|c| c.name).collect();
                let app = LiveApp {
                    app_name: dozer.config.app_name,
                    connections,
                    endpoints,
                };
                Ok(LiveResponse {
                    initialized: true,
                    error_message: None,
                    app: Some(app),
                })
            }
            Err(e) => Ok(LiveResponse {
                initialized: false,
                error_message: Some(e.to_string()),
                app: None,
            }),
        }
    }

    pub fn get_sql(&self) -> Result<SqlResponse, LiveError> {
        let dozer = self.get_dozer()?;
        let sql = dozer.config.sql.clone().unwrap_or_default();
        Ok(SqlResponse { sql })
    }
    pub fn get_endpoints_schemas(&self) -> Result<SchemasResponse, LiveError> {
        let dozer = self.get_dozer()?;
        get_endpoint_schemas(dozer).map_err(|e| LiveError::BuildError(Box::new(e)))
    }
    pub async fn get_source_schemas(
        &self,
        connection_name: String,
    ) -> Result<SchemasResponse, LiveError> {
        let dozer = self.get_dozer()?;
        get_source_schemas(dozer, connection_name)
            .await
            .map_err(|e| LiveError::BuildError(Box::new(e)))
    }

    pub fn generate_dot(&self) -> Result<DotResponse, LiveError> {
        let dozer = self.get_dozer()?;
        generate_dot(dozer).map_err(|e| LiveError::BuildError(Box::new(e)))
    }

    pub fn build_sql(&self, sql: String) -> Result<SchemasResponse, LiveError> {
        let mut dozer = self.get_dozer()?;

        let context = statement_to_pipeline(&sql, &mut AppPipeline::new(), None)
            .map_err(LiveError::PipelineError)?;

        //overwrite sql
        dozer.config.sql = Some(sql);

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

        get_endpoint_schemas(dozer).map_err(|e| LiveError::BuildError(Box::new(e)))
    }

    pub fn run_sql(
        &self,
        sql: String,
        endpoints: Vec<String>,
        sender: tokio::sync::mpsc::Sender<Result<Operation, tonic::Status>>,
    ) -> Result<(), LiveError> {
        let dozer = self.get_dozer()?;

        // kill if a handle already exists
        self.stop_sql_thread();

        let (shutdown_sender, shutdown_receiver) = shutdown::new(&dozer.runtime);
        let _handle = run_sql(dozer, sql, endpoints, sender, shutdown_receiver)
            .map_err(|e| LiveError::BuildError(Box::new(e)))?;
        let mut lock = self.sql_thread.write();
        *lock = Some(shutdown_sender);
        Ok(())
    }

    pub fn stop_sql_thread(&self) {
        let mut lock = self.sql_thread.write();
        if let Some(shutdown) = lock.take() {
            shutdown.shutdown()
        }
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
        get_connector(connection.clone()).map_err(|e| LiveError::BuildError(Box::new(e)))?;

    let (tables, schemas) = connector
        .list_all_schemas()
        .await
        .map_err(|e| LiveError::BuildError(Box::new(e)))?;

    let schemas = schemas
        .into_iter()
        .zip(tables)
        .map(|(s, table)| {
            let schema = s.schema;
            let primary_index = schema.primary_index.into_iter().map(|i| i as i32).collect();
            let schema = Schema {
                primary_index,
                fields: map_field_definitions(schema.fields),
            };
            (table.name, schema)
        })
        .collect();

    Ok(SchemasResponse { schemas })
}

pub fn get_endpoint_schemas(
    dozer: SimpleOrchestrator,
) -> Result<SchemasResponse, OrchestrationError> {
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
    let dag = builder.build(dozer.runtime.clone())?;
    // Populate schemas.
    let dag_schemas = DagSchemas::new(dag)?;

    let schemas = dag_schemas.get_sink_schemas();

    let schemas = schemas
        .into_iter()
        .map(|(name, tuple)| {
            let (schema, _) = tuple;
            let primary_index = schema.primary_index.into_iter().map(|i| i as i32).collect();
            let schema = Schema {
                primary_index,
                fields: map_field_definitions(schema.fields),
            };
            (name, schema)
        })
        .collect();
    Ok(SchemasResponse { schemas })
}

pub fn generate_dot(dozer: SimpleOrchestrator) -> Result<DotResponse, OrchestrationError> {
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
    let dag = builder.build(dozer.runtime.clone())?;
    // Populate schemas.

    let dot_str = dot::Dot::new(dag.graph()).to_string();

    Ok(DotResponse { dot: dot_str })
}

pub fn run_sql(
    dozer: SimpleOrchestrator,
    sql: String,
    endpoints: Vec<String>,
    sender: tokio::sync::mpsc::Sender<Result<Operation, tonic::Status>>,
    shutdown_receiver: ShutdownReceiver,
) -> Result<JoinHandle<()>, OrchestrationError> {
    let mut dozer = dozer;

    let runtime = dozer.runtime.clone();
    //overwrite sql
    dozer.config.sql = Some(sql);

    dozer.config.endpoints = vec![];
    for endpoint in &endpoints {
        dozer.config.endpoints.push(ApiEndpoint {
            name: endpoint.clone(),
            table_name: endpoint.clone(),
            path: format!("/{}", endpoint),
            ..Default::default()
        })
    }

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

    let internal_grpc_config = get_app_grpc_config(&dozer.config);
    let app_server_addr = format!(
        "http://{}:{}",
        internal_grpc_config.host, internal_grpc_config.port
    );
    let (tx, rx) = dozer_types::crossbeam::channel::unbounded::<bool>();
    let pipeline_thread = std::thread::spawn(move || {
        dozer.build(true).unwrap();
        dozer.run_apps(shutdown_receiver, Some(tx), None)
    });
    let endpoint_name = endpoints[0].clone();

    let recv_res = rx.recv();
    if recv_res.is_err() {
        return match pipeline_thread.join() {
            Ok(Err(e)) => Err(e),
            Ok(Ok(())) => panic!("An error must have happened"),
            Err(e) => {
                std::panic::panic_any(e);
            }
        };
    }

    info!("Starting log reader {:?}", endpoint_name);

    let handle = std::thread::spawn(move || {
        runtime.block_on(async {
            let mut log_reader = LogReaderBuilder::new(
                app_server_addr,
                LogReaderOptions::new(endpoint_name.clone()),
            )
            .await
            .unwrap()
            .build(0, None);

            let mut counter = 0;
            loop {
                let (op, _) = log_reader.next_op().await.unwrap();
                counter += 1;

                if let LogOperation::Op { op } = op {
                    let op = map_operation(endpoint_name.clone(), op);
                    sender.send(Ok(op)).await.unwrap();
                }
            }
        });

        pipeline_thread.join().unwrap().unwrap();
    });

    Ok(handle)
}
