use crate::dag::dag::{Dag, Endpoint, NodeType};
use crate::dag::executor_checkpoint::CheckpointMetadataReader;
use crate::dag::executor_local::{ExecutorOptions, MultiThreadedDagExecutor};
use crate::dag::node::NodeHandle;
use crate::dag::tests::dag_recordreader::{
    PassthroughProcessorFactory, PASSTHROUGH_PROCESSOR_INPUT_PORT,
    PASSTHROUGH_PROCESSOR_OUTPUT_PORT,
};
use crate::dag::tests::sinks::{CountingSinkFactory, COUNTING_SINK_INPUT_PORT};
use crate::dag::tests::sources::{StatefulGeneratorSourceFactory, GENERATOR_SOURCE_OUTPUT_PORT};


use std::time::Duration;
use tempdir::TempDir;

macro_rules! chk {
    ($stmt:expr) => {
        $stmt.unwrap_or_else(|e| panic!("{}", e.to_string()))
    };
}

fn build_dag() -> Dag {
    let src = StatefulGeneratorSourceFactory::new(25_000, Duration::from_millis(0));
    let passthrough = PassthroughProcessorFactory::new();
    let sink = CountingSinkFactory::new(25_000);

    let mut dag = Dag::new();

    let SOURCE_ID: NodeHandle = "source".to_string();
    let PASSTHROUGH_ID: NodeHandle = "passthrough".to_string();
    let SINK_ID: NodeHandle = "sink".to_string();

    dag.add_node(NodeType::Source(Box::new(src)), SOURCE_ID.clone());
    dag.add_node(
        NodeType::Processor(Box::new(passthrough)),
        PASSTHROUGH_ID.clone(),
    );
    dag.add_node(NodeType::Sink(Box::new(sink)), SINK_ID.clone());

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

    dag
}

#[test]
fn test_checpoint() {
    let dag = build_dag();

    let tmp_dir = chk!(TempDir::new("example"));
    let exec = chk!(MultiThreadedDagExecutor::start(
        dag,
        tmp_dir.path(),
        ExecutorOptions::default()
    ));

    assert!(exec.join().is_ok());

    let dag_check = build_dag();

    let _chk = chk!(CheckpointMetadataReader::get_checkpoint_metadata(
        tmp_dir.path(),
        &dag_check
    ));
}
