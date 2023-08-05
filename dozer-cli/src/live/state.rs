use clap::Parser;
use dozer_api::grpc::types_helper::map_field_definitions;
use dozer_core::{dag_schemas::DagSchemas, petgraph::dot};
use dozer_ingestion::connectors::get_connector;
use dozer_types::{
    grpc_types::live::{DotResponse, LiveApp, LiveResponse, Schema, SchemasResponse},
    indicatif::MultiProgress,
    parking_lot::RwLock,
};

use crate::{
    cli::{init_dozer, types::Cli},
    errors::OrchestrationError,
    pipeline::PipelineBuilder,
    simple::SimpleOrchestrator,
};

use super::LiveError;

pub struct LiveState {
    dozer: RwLock<Option<SimpleOrchestrator>>,
}

impl LiveState {
    pub fn new() -> Self {
        Self {
            dozer: RwLock::new(None),
        }
    }

    pub fn get_dozer(&self) -> Result<SimpleOrchestrator, LiveError> {
        match self.dozer.read().as_ref() {
            Some(dozer) => Ok(dozer.clone()),
            None => return Err(LiveError::NotInitialized),
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
