use crate::dag::dag::{Dag, Endpoint, NodeType};
use crate::dag::executor_checkpoint::{CheckpointMetadataReader, Consistency};
use crate::dag::executor_local::{ExecutorOptions, MultiThreadedDagExecutor, DEFAULT_PORT_HANDLE};
use crate::dag::node::NodeHandle;
use crate::dag::tests::dag_recordreader::{
    PassthroughProcessorFactory, PASSTHROUGH_PROCESSOR_INPUT_PORT,
    PASSTHROUGH_PROCESSOR_OUTPUT_PORT,
};
use crate::dag::tests::sinks::{CountingSinkFactory, COUNTING_SINK_INPUT_PORT};
use crate::dag::tests::sources::{StatefulGeneratorSourceFactory, GENERATOR_SOURCE_OUTPUT_PORT};
use std::collections::HashMap;

use crate::dag::tests::processors::{DynPortsProcessorFactory, DynPortsSinkFactory};
use crate::storage::lmdb_storage::LmdbEnvironmentManager;
use std::time::Duration;
use tempdir::TempDir;

macro_rules! chk {
    ($stmt:expr) => {
        $stmt.unwrap_or_else(|e| panic!("{}", e.to_string()))
    };
}

fn build_dag() -> Dag {
    let src1 = StatefulGeneratorSourceFactory::new(25_000, Duration::from_millis(0));
    let src2 = StatefulGeneratorSourceFactory::new(10_000, Duration::from_millis(0));
    let proc = DynPortsProcessorFactory::new(1, vec![1, 2], vec![DEFAULT_PORT_HANDLE]);
    let sink = CountingSinkFactory::new(0);

    let mut dag = Dag::new();

    let source_id_1: NodeHandle = "source1".to_string();
    let source_id_2: NodeHandle = "source2".to_string();
    let proc_id: NodeHandle = "proc".to_string();
    let sink_id: NodeHandle = "sink".to_string();

    dag.add_node(NodeType::Source(Box::new(src1)), source_id_1.clone());
    dag.add_node(NodeType::Source(Box::new(src2)), source_id_2.clone());
    dag.add_node(NodeType::Processor(Box::new(proc)), proc_id.clone());
    dag.add_node(NodeType::Sink(Box::new(sink)), sink_id.clone());

    assert!(dag
        .connect(
            Endpoint::new(source_id_1, GENERATOR_SOURCE_OUTPUT_PORT),
            Endpoint::new(proc_id.clone(), 1),
        )
        .is_ok());

    assert!(dag
        .connect(
            Endpoint::new(source_id_2, GENERATOR_SOURCE_OUTPUT_PORT),
            Endpoint::new(proc_id.clone(), 2),
        )
        .is_ok());

    assert!(dag
        .connect(
            Endpoint::new(proc_id, DEFAULT_PORT_HANDLE),
            Endpoint::new(sink_id, COUNTING_SINK_INPUT_PORT),
        )
        .is_ok());

    dag
}

#[test]
fn test_checpoint_consistency() {
    let dag = build_dag();

    let tmp_dir = chk!(TempDir::new("example"));
    let exec = chk!(MultiThreadedDagExecutor::start(
        dag,
        tmp_dir.path(),
        ExecutorOptions::default()
    ));

    assert!(exec.join().is_ok());

    let dag_check = build_dag();

    let r = chk!(CheckpointMetadataReader::new(&dag_check, tmp_dir.path()));
    let c = r.get_dependency_tree_consistency();

    assert!(matches!(
        c.get("source").unwrap(),
        Consistency::FullyConsistent(24999)
    ));

    LmdbEnvironmentManager::remove(tmp_dir.path(), "passthrough");
    let r = chk!(CheckpointMetadataReader::new(&dag_check, tmp_dir.path()));
    let c = r.get_dependency_tree_consistency();

    let mut expected: HashMap<u64, Vec<NodeHandle>> = HashMap::new();
    expected.insert(24999_u64, vec!["source".to_string(), "sink".to_string()]);
    expected.insert(0_u64, vec!["passthrough".to_string()]);

    assert!(matches!(
        c.get("source").unwrap(),
        Consistency::PartiallyConsistent(expected)
    ));
}
