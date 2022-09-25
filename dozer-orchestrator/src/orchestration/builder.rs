use core::fmt;
use dozer_shared::types::TableInfo;
use std::error::Error;

use super::{
    models::{connection::Connection, endpoint::Endpoint as EndpointModel, source::Source},
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
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::{rc::Rc, sync::Arc};

    use crate::orchestration::models::{
        connection::Authentication::PostgresAuthentication,
        source::{MasterHistoryConfig, RefreshConfig},
    };
    use crate::orchestration::{
        models::{
            connection::{Authentication, Connection, DBType},
            source::{HistoryType, Source},
        },
        orchestrator::PgSource,
        sample::{SampleProcessor, SampleSink},
    };
    use dozer_core::dag::{
        channel::LocalNodeChannel,
        dag::{Dag, Endpoint, NodeType},
        executor::{MemoryExecutionContext, MultiThreadedDagExecutor},
    };
    use dozer_ingestion::connectors::{postgres::connector::PostgresConfig, storage::RocksConfig};
    #[test]
    fn run_workflow() {
        let connection: Connection = Connection {
            db_type: DBType::Postgres,
            authentication: PostgresAuthentication {
                user: "postgres".to_string(),
                password: "postgres".to_string(),
                host: "localhost".to_string(),
                port: 5432,
                database: "pagila".to_string(),
            },
            name: "postgres connection".to_string(),
            id: None,
        };
        let source = Source {
            id: None,
            name: "actor_source".to_string(),
            dest_table_name: "ACTOR_SOURCE".to_string(),
            source_table_name: "actor".to_string(),
            connection,
            history_type: HistoryType::Master(MasterHistoryConfig::AppendOnly {
                unique_key_field: "actor_id".to_string(),
                open_date_field: "last_updated".to_string(),
                closed_date_field: "last_updated".to_string(),
            }),
            refresh_config: RefreshConfig::RealTime,
        };
        let storage_config = RocksConfig {
            path: "./db/embedded".to_string(),
        };
        let mut sources = Vec::new();
        sources.push(source);
        let mut pg_sources = Vec::new();
        sources
            .clone()
            .iter()
            .for_each(|source| match source.connection.authentication.clone() {
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
            });
        let proc = SampleProcessor::new(2, None, None);
        let sink = SampleSink::new(2, None);
        let mut dag = Dag::new();
        let proc_handle = dag.add_node(NodeType::Processor(Arc::new(proc)));
        let sink_handle = dag.add_node(NodeType::Sink(Arc::new(sink)));

        pg_sources.clone().iter().for_each(|pg_source| {
            let src_handle = dag.add_node(NodeType::Source(Arc::new(pg_source.clone())));
            dag.connect(
                Endpoint::new(src_handle, None),
                Endpoint::new(proc_handle, None),
                Box::new(LocalNodeChannel::new(10000)),
            )
            .unwrap();
        });

        dag.connect(
            Endpoint::new(proc_handle, None),
            Endpoint::new(sink_handle, None),
            Box::new(LocalNodeChannel::new(10000)),
        )
        .unwrap();

        let exec = MultiThreadedDagExecutor::new(Rc::new(dag));
        let ctx = Arc::new(MemoryExecutionContext::new());
        let _res = exec.start(ctx);
        // assert!(_res.is_ok());
    }
}
