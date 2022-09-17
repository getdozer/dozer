mod orchestrator;
mod sample;
use std::{rc::Rc, sync::Arc};

use dozer_core::dag::{
    channel::LocalNodeChannel,
    dag::{Dag, Endpoint, NodeType},
    executor::{MemoryExecutionContext, MultiThreadedDagExecutor},
};
use dozer_ingestion::connectors::{postgres::connector::PostgresConfig, storage::RocksConfig};
use orchestrator::PgSource;
use sample::{SampleProcessor, SampleSink};
fn main() {
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
