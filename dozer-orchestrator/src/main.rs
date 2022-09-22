mod orchestrator;
mod sample;
use dozer_core::dag::{
    channel::LocalNodeChannel,
    dag::{Dag, Endpoint, NodeType},
    executor::{MemoryExecutionContext, MultiThreadedDagExecutor},
};
use dozer_ingestion::connectors::{postgres::connector::PostgresConfig, storage::RocksConfig};
use dozer_orchestrator::orchestration::{
    builder::Dozer,
    models::{
        connection::{Authentication::PostgresAuthentication, Connection, DBType},
        source::{HistoryType, MasterHistoryConfig, Source, RefreshConfig},
    },
};
use orchestrator::PgSource;
use sample::{SampleProcessor, SampleSink};
use std::{rc::Rc, sync::Arc};

fn main() {
    // let connection: Connection = Connection {
    //     db_type: DBType::Postgres,
    //     authentication: PostgresAuthentication {
    //         user: "postgres".to_string(),
    //         password: "postgres".to_string(),
    //         host: "localhost".to_string(),
    //         port: 5432,
    //         database: "pagila".to_string(),
    //     },
    //     name: "postgres connection".to_string(),
    //     id: None,
    // };
    // let source: Source = Source {
    //     id: None,
    //     name: "source name".to_string(),
    //     dest_table_name: "SOURCE_NAME".to_string(),
    //     connection,
    //     history_type: HistoryType::Master(MasterHistoryConfig::AppendOnly {
    //         unique_key_field: "id".to_string(),
    //         open_date_field: "created_date".to_string(),
    //         closed_date_field: "updated_date".to_string(),
    //     }),
    //     refresh_config: RefreshConfig::RealTime,
    // };
    // let dozer = Dozer::new();
    // let sources = Vec::new();
    // sources.push(source);
    // dozer.add_sources(sources);
    // dozer.save();

    let storage_config = RocksConfig {
        path: "./db/embedded".to_string(),
    };
    let postgres_config = PostgresConfig {
        name: "test_c".to_string(),
        // tables: Some(vec!["actor".to_string()]),
        tables: None,
        conn_str: "host=127.0.0.1 port=5432 user=postgres dbname=pagila".to_string(),
        // conn_str: "host=127.0.0.1 port=5432 user=postgres dbname=large_film".to_string(),
    };

    let src = PgSource::new(storage_config, postgres_config);
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
}
