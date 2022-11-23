use crate::dag::dag::{Dag, Endpoint, NodeType};
use crate::dag::executor_local::{ExecutorOptions, MultiThreadedDagExecutor, DEFAULT_PORT_HANDLE};
use crate::dag::tests::processors::{
    DynPortsProcessorFactory, DynPortsSinkFactory, DynPortsSourceFactory,
};
use std::fs;
use tempdir::TempDir;

macro_rules! chk {
    ($stmt:expr) => {
        $stmt.unwrap_or_else(|e| panic!("{}", e.to_string()))
    };
}

#[test]
fn test_run_dag() {
    // log4rs::init_file("../log4rs.sample.yaml", Default::default())
    //     .unwrap_or_else(|_e| panic!("Unable to find log4rs config file"));

    let src = DynPortsSourceFactory::new(1, vec![DEFAULT_PORT_HANDLE]);
    let proc1 =
        DynPortsProcessorFactory::new(1, vec![DEFAULT_PORT_HANDLE], vec![DEFAULT_PORT_HANDLE]);
    let proc2 =
        DynPortsProcessorFactory::new(1, vec![DEFAULT_PORT_HANDLE], vec![DEFAULT_PORT_HANDLE]);
    let sink = DynPortsSinkFactory::new(1, vec![DEFAULT_PORT_HANDLE]);

    let mut dag = Dag::new();

    dag.add_node(NodeType::StatelessSource(Box::new(src)), 1.to_string());
    dag.add_node(NodeType::StatefulProcessor(Box::new(proc1)), 2.to_string());
    dag.add_node(NodeType::StatefulProcessor(Box::new(proc2)), 3.to_string());
    dag.add_node(NodeType::StatelessSink(Box::new(sink)), 4.to_string());

    let src_to_proc1 = dag.connect(
        Endpoint::new(1.to_string(), DEFAULT_PORT_HANDLE),
        Endpoint::new(2.to_string(), DEFAULT_PORT_HANDLE),
    );
    assert!(src_to_proc1.is_ok());

    let proc1_to_sink = dag.connect(
        Endpoint::new(2.to_string(), DEFAULT_PORT_HANDLE),
        Endpoint::new(3.to_string(), DEFAULT_PORT_HANDLE),
    );
    assert!(proc1_to_sink.is_ok());

    let proc1_to_sink = dag.connect(
        Endpoint::new(3.to_string(), DEFAULT_PORT_HANDLE),
        Endpoint::new(4.to_string(), DEFAULT_PORT_HANDLE),
    );
    assert!(proc1_to_sink.is_ok());

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
}
