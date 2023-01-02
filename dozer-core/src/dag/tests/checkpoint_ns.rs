use crate::chk;
use crate::dag::dag::{Dag, Endpoint, NodeType, DEFAULT_PORT_HANDLE};

use crate::dag::executor::{DagExecutor, ExecutorOptions};
use crate::dag::node::NodeHandle;
use crate::dag::tests::common::init_log4rs;
use crate::dag::tests::dag_base_run::NoopJoinProcessorFactory;
use crate::dag::tests::sinks::{CountingSinkFactory, COUNTING_SINK_INPUT_PORT};
use crate::dag::tests::sources::{GeneratorSourceFactory, GENERATOR_SOURCE_OUTPUT_PORT};
use serial_test::serial;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc};
use tempdir::TempDir;

#[test]
#[serial]
fn test_checpoint_consistency_ns() {
    // dozer_tracing::init_telemetry(false).unwrap();

    const MESSAGES_COUNT: u64 = 25_000;

    init_log4rs();
    let mut dag = Dag::new();

    let sources: Vec<NodeHandle> = vec![
        NodeHandle::new(None, "src1".to_string()),
        NodeHandle::new(None, "src2".to_string()),
        NodeHandle::new(None, "src3".to_string()),
        NodeHandle::new(None, "src4".to_string()),
        NodeHandle::new(None, "src5".to_string()),
    ];

    let latch = Arc::new(AtomicBool::new(true));

    for src_handle in &sources {
        dag.add_node(
            NodeType::Source(Arc::new(GeneratorSourceFactory::new(
                MESSAGES_COUNT,
                latch.clone(),
                true,
            ))),
            src_handle.clone(),
        );
    }

    // Create sources.len()-1 sub dags
    for i in 0..sources.len() - 1 {
        let mut child_dag = Dag::new();

        let proc_handle = NodeHandle::new(None, "proc".to_string());
        let sink_handle = NodeHandle::new(None, "sink".to_string());

        child_dag.add_node(
            NodeType::Processor(Arc::new(NoopJoinProcessorFactory {})),
            proc_handle.clone(),
        );
        child_dag.add_node(
            NodeType::Sink(Arc::new(CountingSinkFactory::new(
                MESSAGES_COUNT * 2,
                latch.clone(),
            ))),
            sink_handle.clone(),
        );
        chk!(child_dag.connect(
            Endpoint::new(proc_handle, DEFAULT_PORT_HANDLE),
            Endpoint::new(sink_handle, COUNTING_SINK_INPUT_PORT),
        ));

        // Merge the DAG with the parent dag
        dag.merge(Some(i as u16), child_dag);

        chk!(dag.connect(
            Endpoint::new(sources[i].clone(), GENERATOR_SOURCE_OUTPUT_PORT),
            Endpoint::new(NodeHandle::new(Some(i as u16), "proc".to_string()), 1),
        ));
        chk!(dag.connect(
            Endpoint::new(sources[i + 1].clone(), GENERATOR_SOURCE_OUTPUT_PORT),
            Endpoint::new(NodeHandle::new(Some(i as u16), "proc".to_string()), 2),
        ));
    }

    let tmp_dir = chk!(TempDir::new("test"));
    let mut executor = chk!(DagExecutor::new(
        &dag,
        tmp_dir.path(),
        ExecutorOptions::default(),
        Arc::new(AtomicBool::new(true))
    ));

    chk!(executor.start());
    assert!(executor.join().is_ok());
}
