use std::fs;
use dozer_types::types::{FieldDefinition, FieldType, Schema};
use crate::dag::dag::{Dag, Endpoint, NodeType, PortDirection};
use crate::dag::mt_executor::{DefaultPortHandle, MultiThreadedDagExecutor, SchemaKey};
use crate::dag::tests::processors::{TestProcessorFactory, TestSinkFactory, TestSourceFactory};
use crate::state::lmdb::LmdbStateStoreManager;


#[test]
fn test_run_dag() {

    fs::remove_dir_all("data");
    fs::create_dir("data");

    let src = TestSourceFactory::new(1, vec![DefaultPortHandle]);
    let proc = TestProcessorFactory::new(1, vec![DefaultPortHandle], vec![DefaultPortHandle]);
    let sink = TestSinkFactory::new(1, vec![DefaultPortHandle]);

    let mut dag = Dag::new();

    dag.add_node(NodeType::Source(Box::new(src)), 1.to_string());
    dag.add_node(NodeType::Processor(Box::new(proc)), 2.to_string());
    dag.add_node(NodeType::Sink(Box::new(sink)), 3.to_string());

    let src_to_proc1 = dag.connect(
        Endpoint::new(1.to_string(), DefaultPortHandle),
        Endpoint::new(2.to_string(), DefaultPortHandle)
    );
    assert!(src_to_proc1.is_ok());

    let proc1_to_sink = dag.connect(
        Endpoint::new(2.to_string(), DefaultPortHandle),
        Endpoint::new(3.to_string(), DefaultPortHandle)
    );
    assert!(proc1_to_sink.is_ok());

    let exec = MultiThreadedDagExecutor::new( 100000);
    let sm = LmdbStateStoreManager::new(
        "data".to_string(),
        1024*1024*1024*5,
        20_000
    ).unwrap();

    assert!(exec.start(dag, sm).is_ok());

    fs::remove_dir_all("data");
}