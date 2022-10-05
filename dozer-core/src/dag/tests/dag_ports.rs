use dozer_types::types::{FieldDefinition, FieldType, Schema};
use crate::dag::dag::{Dag, Endpoint, NodeType, PortDirection};
use crate::dag::mt_executor::{DefaultPortHandle, MultiThreadedDagExecutor, SchemaKey};
use crate::dag::tests::processors::{TestProcessorFactory, TestSinkFactory, TestSourceFactory};
use crate::state::lmdb::LmdbStateStoreManager;


macro_rules! test_ports {
    ($id:ident, $out_ports:expr, $in_ports:expr, $from_port:expr, $to_port:expr, $expect:expr) => {
        #[test]
        fn $id() {
            let src = TestSourceFactory::new(1, $out_ports);
            let proc = TestProcessorFactory::new(2, $in_ports, vec![DefaultPortHandle]);

            let mut dag = Dag::new();

            dag.add_node(NodeType::Source(Box::new(src)), 1.to_string());
            dag.add_node(NodeType::Processor(Box::new(proc)), 2.to_string());

            let res = dag.connect(
                Endpoint::new(1.to_string(), $from_port),
                Endpoint::new(2.to_string(), $to_port)
            );

            assert!(res.is_ok() == $expect)
        }
    };
}

test_ports!(
    test_none_ports,
    vec![DefaultPortHandle],
    vec![DefaultPortHandle],
    DefaultPortHandle,
    DefaultPortHandle,
    true);

test_ports!(
    test_matching_ports,
    vec![1],
    vec![2],
    1,
    2,
    true
);
test_ports!(
    test_not_matching_ports,
    vec![2],
    vec![1],
    1,
    2,
    false
);
test_ports!(
    test_not_default_port,
    vec![2],
    vec![1],
    DefaultPortHandle,
    2,
    false
);
test_ports!(
    test_not_default_port2,
    vec![DefaultPortHandle],
    vec![1],
    1,
    2,
    false
);
test_ports!(
    test_not_default_port3,
    vec![DefaultPortHandle],
    vec![DefaultPortHandle],
    DefaultPortHandle,
    2,
    false
);
