use crate::dag::dag::{Dag, Endpoint, NodeType};
use crate::dag::executor_local::DEFAULT_PORT_HANDLE;
use crate::dag::tests::processors::{DynPortsProcessorFactory, DynPortsSourceFactory};

macro_rules! test_ports {
    ($id:ident, $out_ports:expr, $in_ports:expr, $from_port:expr, $to_port:expr, $expect:expr) => {
        #[test]
        fn $id() {
            let src = DynPortsSourceFactory::new(1, $out_ports);
            let proc = DynPortsProcessorFactory::new(2, $in_ports, vec![DEFAULT_PORT_HANDLE]);

            let mut dag = Dag::new();

            dag.add_node(NodeType::Source(Box::new(src)), 1.to_string());
            dag.add_node(NodeType::Processor(Box::new(proc)), 2.to_string());

            let res = dag.connect(
                Endpoint::new(1.to_string(), $from_port),
                Endpoint::new(2.to_string(), $to_port),
            );

            assert!(res.is_ok() == $expect)
        }
    };
}

test_ports!(
    test_none_ports,
    vec![DEFAULT_PORT_HANDLE],
    vec![DEFAULT_PORT_HANDLE],
    DEFAULT_PORT_HANDLE,
    DEFAULT_PORT_HANDLE,
    true
);

test_ports!(test_matching_ports, vec![1], vec![2], 1, 2, true);
test_ports!(test_not_matching_ports, vec![2], vec![1], 1, 2, false);
test_ports!(
    test_not_default_port,
    vec![2],
    vec![1],
    DEFAULT_PORT_HANDLE,
    2,
    false
);

test_ports!(
    test_not_default_port2,
    vec![DEFAULT_PORT_HANDLE],
    vec![1],
    1,
    2,
    false
);
test_ports!(
    test_not_default_port3,
    vec![DEFAULT_PORT_HANDLE],
    vec![DEFAULT_PORT_HANDLE],
    DEFAULT_PORT_HANDLE,
    2,
    false
);

#[test]
fn test_dag_merge() {
    let src = DynPortsSourceFactory::new(1, vec![DEFAULT_PORT_HANDLE]);
    let proc =
        DynPortsProcessorFactory::new(2, vec![DEFAULT_PORT_HANDLE], vec![DEFAULT_PORT_HANDLE]);

    let mut dag = Dag::new();

    dag.add_node(NodeType::Source(Box::new(src)), 1.to_string());
    dag.add_node(NodeType::Processor(Box::new(proc)), 2.to_string());

    let mut new_dag: Dag = Dag::new();
    new_dag.merge("test".to_string(), dag);

    let res = new_dag.connect(
        Endpoint::new("1".to_string(), DEFAULT_PORT_HANDLE),
        Endpoint::new("2".to_string(), DEFAULT_PORT_HANDLE),
    );
    assert!(res.is_err());

    let res = new_dag.connect(
        Endpoint::new("test_1".to_string(), DEFAULT_PORT_HANDLE),
        Endpoint::new("test_2".to_string(), DEFAULT_PORT_HANDLE),
    );
    assert!(res.is_ok())
}
