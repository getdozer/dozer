use crate::dag::dag::{Dag, Endpoint, NodeType};
use crate::dag::executor_local::{ExecutorOptions, MultiThreadedDagExecutor};
use crate::dag::node::NodeHandle;
use crate::dag::tests::dag_recordreader::{
    PassthroughProcessorFactory, PASSTHROUGH_PROCESSOR_INPUT_PORT,
    PASSTHROUGH_PROCESSOR_OUTPUT_PORT,
};
use crate::dag::tests::sinks::{CountingSinkFactory, COUNTING_SINK_INPUT_PORT};
use crate::dag::tests::sources::{StatefulGeneratorSourceFactory, GENERATOR_SOURCE_OUTPUT_PORT};
use crate::storage::lmdb_storage::LmdbEnvironmentManager;
use std::fs;
use std::time::Duration;
use tempdir::TempDir;

macro_rules! chk {
    ($stmt:expr) => {
        $stmt.unwrap_or_else(|e| panic!("{}", e.to_string()))
    };
}

#[test]
fn test_checpoint() {
    let src = StatefulGeneratorSourceFactory::new(25_000, Duration::from_millis(0));
    let passthrough = PassthroughProcessorFactory::new();
    let sink = CountingSinkFactory::new(25_000);

    let mut dag = Dag::new();

    let SOURCE_ID: NodeHandle = "source".to_string();
    let PASSTHROUGH_ID: NodeHandle = "passthrough".to_string();
    let SINK_ID: NodeHandle = "sink".to_string();

    dag.add_node(NodeType::StatefulSource(Box::new(src)), SOURCE_ID.clone());
    dag.add_node(
        NodeType::StatefulProcessor(Box::new(passthrough)),
        PASSTHROUGH_ID.clone(),
    );
    dag.add_node(NodeType::StatefulSink(Box::new(sink)), SINK_ID.clone());

    assert!(dag
        .connect(
            Endpoint::new(SOURCE_ID, GENERATOR_SOURCE_OUTPUT_PORT),
            Endpoint::new(PASSTHROUGH_ID.clone(), PASSTHROUGH_PROCESSOR_INPUT_PORT),
        )
        .is_ok());

    assert!(dag
        .connect(
            Endpoint::new(PASSTHROUGH_ID, PASSTHROUGH_PROCESSOR_OUTPUT_PORT),
            Endpoint::new(SINK_ID, COUNTING_SINK_INPUT_PORT),
        )
        .is_ok());

    let tmp_dir = chk!(TempDir::new("example"));
    if tmp_dir.path().exists() {
        chk!(fs::remove_dir_all(tmp_dir.path()));
    }
    chk!(fs::create_dir(tmp_dir.path()));

    let exec = chk!(MultiThreadedDagExecutor::start(
        dag,
        tmp_dir.path(),
        ExecutorOptions::default()
    ));

    assert!(exec.join().is_ok());

    let r = LmdbEnvironmentManager::exists(tmp_dir.path(), "source");
    println!("dd");
}
