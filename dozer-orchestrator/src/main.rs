mod sample;
use dozer_core::dag::{
    channel::LocalNodeChannel,
    dag::{Dag, Endpoint, NodeType},
    executor::{MemoryExecutionContext, MultiThreadedDagExecutor},
};
use dozer_ingestion::connectors::{postgres::connector::PostgresConfig, storage::RocksConfig};
use dozer_orchestrator::orchestration::{
    builder::{ Dozer},
    db::service::DbPersistentService,
    models::{
        connection::{Authentication::PostgresAuthentication, Connection, DBType},
        source::{HistoryType, MasterHistoryConfig, RefreshConfig, Source},
    },
};

use orchestrator::PgSource;
use sample::{SampleProcessor, SampleSink};
use std::{error::Error, rc::Rc, sync::Arc};

fn main() {
    let db_url = "dozer.db";
    let persistent_service: DbPersistentService = DbPersistentService::new(db_url.to_owned());

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
    Dozer::test_connection(connection.to_owned()).unwrap();

    let connection_id = persistent_service
        .save_connection(connection.clone())
        .unwrap();
    let connection = persistent_service.read_connection(connection_id).unwrap();
    let source = Source {
        id: None,
        name: "source name".to_string(),
        dest_table_name: "SOURCE_NAME".to_string(),
        connection,
        history_type: HistoryType::Master(MasterHistoryConfig::AppendOnly {
            unique_key_field: "id".to_string(),
            open_date_field: "created_date".to_string(),
            closed_date_field: "updated_date".to_string(),
        }),
        refresh_config: RefreshConfig::RealTime,
    };
    let mut dozer = Dozer::new();
    let mut sources = Vec::new();
    sources.push(source);
    dozer.add_sources(sources);
    dozer.run();

    // let storage_config = RocksConfig {
    //     path: "./db/embedded".to_string(),
    // };
    // let postgres_config = PostgresConfig {
    //     name: "test_c".to_string(),
    //     // tables: Some(vec!["actor".to_string()]),
    //     tables: None,
    //     conn_str: "host=127.0.0.1 port=5432 user=postgres dbname=pagila".to_string(),
    //     // conn_str: "host=127.0.0.1 port=5432 user=postgres dbname=large_film".to_string(),
    // };

    // let src = PgSource::new(storage_config, postgres_config);
    // let proc = SampleProcessor::new(2, None, None);
    // let sink = SampleSink::new(2, None);

    // let mut dag = Dag::new();

    // let src_handle = dag.add_node(NodeType::Source(Arc::new(src)));
    // let proc_handle = dag.add_node(NodeType::Processor(Arc::new(proc)));
    // let sink_handle = dag.add_node(NodeType::Sink(Arc::new(sink)));

    // dag.connect(
    //     Endpoint::new(src_handle, None),
    //     Endpoint::new(proc_handle, None),
    //     Box::new(LocalNodeChannel::new(10000)),
    // )
    // .unwrap();

    // dag.connect(
    //     Endpoint::new(proc_handle, None),
    //     Endpoint::new(sink_handle, None),
    //     Box::new(LocalNodeChannel::new(10000)),
    // )
    // .unwrap();

    // let exec = MultiThreadedDagExecutor::new(Rc::new(dag));
    // let ctx = Arc::new(MemoryExecutionContext::new());

    // let _res = exec.start(ctx);
}
