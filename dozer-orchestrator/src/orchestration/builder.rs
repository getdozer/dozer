use core::fmt;
use dozer_core::dag::{dag::{Dag, NodeType, Endpoint}, channel::LocalNodeChannel, executor::{MultiThreadedDagExecutor, MemoryExecutionContext}};
use dozer_ingestion::connectors::{
    connector::Connector, postgres::connector::PostgresConfig, storage::RocksConfig,
};
use dozer_shared::types::TableInfo;
use std::{error::Error, ops::Deref, sync::Arc, rc::Rc};

use super::{
    models::{
        connection::{Authentication, Connection},
        source::Source,
        endpoint::Endpoint as EndpointModel
    },
    orchestrator::PgSource,
    sample::{SampleProcessor, SampleSink},
    services::connection::ConnectionService,
};

pub struct Dozer {
    sources: Option<Vec<Source>>,
    endpoints: Option<Vec<EndpointModel>>,
}
#[derive(Debug, Clone)]
struct OrchestratorError {
    message: String,
}
impl fmt::Display for OrchestratorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OrchestratorError {:?}", self.message)
    }
}
impl Error for OrchestratorError {}

impl Dozer {
    pub fn test_connection(input: Connection) -> Result<(), Box<dyn Error>> {
        let connection_service = ConnectionService::new(input);
        return connection_service.test_connection();
    }

    pub fn get_schema(input: Connection) -> Result<Vec<TableInfo>, Box<dyn Error>> {
        let connection_service = ConnectionService::new(input);
        return connection_service.get_schema();
    }
}

impl Dozer {
    pub fn new() -> Self {
        Self {
            sources: None,
            endpoints: None,
        }
    }
    pub fn add_sources(&mut self, sources: Vec<Source>) -> &mut Self {
        let my_source = self.sources.clone();
        match my_source {
            Some(current_data) => {
                let new_source = [current_data, sources.clone()].concat();
                self.sources = Some(new_source);
                return self;
            }
            None => {
                self.sources = Some(sources);
                return self;
            }
        }
    }
    pub fn add_endpoints(&mut self, endpoints: Vec<EndpointModel>) -> &mut Self {
        let my_endpoint = self.endpoints.clone();
        match my_endpoint {
            Some(current_data) => {
                let new_endponts = [current_data, endpoints.clone()].concat();
                self.endpoints = Some(new_endponts);
                return self;
            }
            None => {
                self.endpoints = Some(endpoints);
                return self;
            }
        }
    }

    pub fn run(&mut self) -> Result<&mut Self, Box<dyn Error>> {
        let storage_config = RocksConfig {
            path: "./db/embedded".to_string(),
        };
        if self.sources.is_none() {
            return Err(Box::new(OrchestratorError {
                message: "No source provided".to_owned(),
            }));
        }
        let mut pg_sources = Vec::new();
        self.sources.clone().unwrap().iter().for_each(|source| {
            match source.connection.authentication.clone() {
                Authentication::PostgresAuthentication {
                    user,
                    password,
                    host,
                    port,
                    database,
                } => {
                    let conn_str = format!(
                        "host={} port={} user={} dbname={} password={}",
                        host, port, user, database, password,
                    );
                    let postgres_config = PostgresConfig {
                        name: source.connection.name.clone(),
                        tables: None,
                        conn_str: conn_str,
                    };
                    pg_sources.push(PgSource::new(storage_config.clone(), postgres_config))
                }
            }
        });
        let src = pg_sources.first().unwrap().clone();
        let proc = SampleProcessor::new(2, None, None);
        let sink = SampleSink::new(2, None);
        let mut dag = Dag::new();

        let src_handle = dag.add_node(NodeType::Source(Arc::new(src)));
        let proc_handle = dag.add_node(NodeType::Processor(Arc::new(proc)));
        let sink_handle = dag.add_node(NodeType::Sink(Arc::new(sink)));

        dag.connect(
            Endpoint::new(src_handle, None),
            Endpoint::new(proc_handle, None),
            Box::new(LocalNodeChannel::new(10000)),
        )
        .unwrap();

        dag.connect(
            Endpoint::new(proc_handle, None),
            Endpoint::new(sink_handle, None),
            Box::new(LocalNodeChannel::new(10000)),
        )
        .unwrap();

        let exec = MultiThreadedDagExecutor::new(Rc::new(dag));
        let ctx = Arc::new(MemoryExecutionContext::new());
        let _res = exec.start(ctx);
        todo!()

        // let src = PgSource::new(storage_config, postgres_config);
    }
}
