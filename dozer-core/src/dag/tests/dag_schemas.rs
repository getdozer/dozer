use dozer_types::types::{FieldDefinition, FieldType, Schema};
use crate::dag::dag::{Dag, Endpoint, NodeType, PortDirection};
use crate::dag::mt_executor::{DefaultPortHandle, MultiThreadedDagExecutor, SchemaKey};
use crate::dag::tests::processors::{TestProcessorFactory, TestSinkFactory, TestSourceFactory};

///                   ┌───────────┐          ┌──────────┐
///      ┌────────────►           ├──────────►          ├─────────┐
///      │110      201│    200    │210    401│   400    │410      │501
/// ┌────┴─────┐      │           │          │          │   ┌─────▼─────┐      ┌───────────┐
/// │          │      └───────────┘          └──────────┘   │           │   601│           │
/// │   100    │                                            │   500     ├──────►    600    │
/// │          │      ┌───────────┐                         │           │510   │           │
/// └────┬─────┘      │           │                         └─────▲─────┘      └───────────┘
///      │120         │    300    │                               │502
///      └────────────►           ├───────────────────────────────┘
///                301└───────────┘310
///
#[test]
fn test_schemas_compute() {


    let src_100 = TestSourceFactory::new(100, vec![110, 120]);
    let proc_200 = TestProcessorFactory::new(200, vec![201], vec![210]);
    let proc_300 = TestProcessorFactory::new(300, vec![301], vec![310]);
    let proc_400 = TestProcessorFactory::new(400, vec![401], vec![410]);
    let proc_500 = TestProcessorFactory::new(500, vec![501, 502], vec![510]);
    let sink_600 = TestSinkFactory::new(600, vec![601]);

    let mut dag = Dag::new();

    dag.add_node(NodeType::Source(Box::new(src_100)), 100);
    dag.add_node(NodeType::Processor(Box::new(proc_200)), 200);
    dag.add_node(NodeType::Processor(Box::new(proc_300)), 300);
    dag.add_node(NodeType::Processor(Box::new(proc_400)), 400);
    dag.add_node(NodeType::Processor(Box::new(proc_500)), 500);
    dag.add_node(NodeType::Sink(Box::new(sink_600)), 600);

    assert!(dag.connect(Endpoint::new(100, 110), Endpoint::new(200, 201)).is_ok());
    assert!(dag.connect(Endpoint::new(100, 120), Endpoint::new(300, 301)).is_ok());
    assert!(dag.connect(Endpoint::new(200, 210), Endpoint::new(400, 401)).is_ok());
    assert!(dag.connect(Endpoint::new(300, 310), Endpoint::new(500, 502)).is_ok());
    assert!(dag.connect(Endpoint::new(400, 410), Endpoint::new(500, 501)).is_ok());
    assert!(dag.connect(Endpoint::new(500, 510), Endpoint::new(600, 601)).is_ok());

    let exec = MultiThreadedDagExecutor::new( 100000);
    let r = exec.get_schemas_map(&dag).unwrap();

    assert_eq!(
        r.get(&SchemaKey::new(200, 210, PortDirection::Output)).unwrap().clone(),
        Schema::empty()
            .field(FieldDefinition::new("node_100_port_110".to_string(), FieldType::String, false), false, false)
            .field(FieldDefinition::new("node_200_port_210".to_string(), FieldType::String, false), false, false)
            .clone()
    );

    assert_eq!(
        r.get(&SchemaKey::new(600, 601, PortDirection::Input)).unwrap().clone(),
        Schema::empty()

            .field(FieldDefinition::new("node_100_port_110".to_string(), FieldType::String, false), false, false)
            .field(FieldDefinition::new("node_200_port_210".to_string(), FieldType::String, false), false, false)
            .field(FieldDefinition::new("node_400_port_410".to_string(), FieldType::String, false), false, false)

            .field(FieldDefinition::new("node_100_port_120".to_string(), FieldType::String, false), false, false)
            .field(FieldDefinition::new("node_300_port_310".to_string(), FieldType::String, false), false, false)
            .field(FieldDefinition::new("node_500_port_510".to_string(), FieldType::String, false), false, false)
            .clone()
    );


}