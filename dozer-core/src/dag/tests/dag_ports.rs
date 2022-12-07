use crate::dag::dag::{Dag, Endpoint, NodeType, DEFAULT_PORT_HANDLE};
use crate::dag::errors::ExecutionError;
use crate::dag::node::{
    NodeHandle, OutputPortDef, OutputPortDefOptions, PortHandle, Processor, ProcessorFactory,
    Source, SourceFactory,
};
use dozer_types::types::Schema;
use std::collections::HashMap;
use std::sync::Arc;

pub struct DynPortsSourceFactory {
    output_ports: Vec<PortHandle>,
}

impl DynPortsSourceFactory {
    pub fn new(output_ports: Vec<PortHandle>) -> Self {
        Self { output_ports }
    }
}

impl SourceFactory for DynPortsSourceFactory {
    fn get_output_schema(&self, _port: &PortHandle) -> Result<Schema, ExecutionError> {
        todo!()
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        self.output_ports
            .iter()
            .map(|p| OutputPortDef::new(*p, OutputPortDefOptions::default()))
            .collect()
    }

    fn build(&self, input_schemas: HashMap<PortHandle, Schema>) -> Box<dyn Source> {
        todo!()
    }
}

pub struct DynPortsProcessorFactory {
    input_ports: Vec<PortHandle>,
    output_ports: Vec<PortHandle>,
}

impl DynPortsProcessorFactory {
    pub fn new(input_ports: Vec<PortHandle>, output_ports: Vec<PortHandle>) -> Self {
        Self {
            input_ports,
            output_ports,
        }
    }
}

impl ProcessorFactory for DynPortsProcessorFactory {
    fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        _input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, ExecutionError> {
        todo!()
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        self.input_ports.clone()
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        self.output_ports
            .iter()
            .map(|p| OutputPortDef::new(*p, OutputPortDefOptions::default()))
            .collect()
    }

    fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
        output_schemas: HashMap<PortHandle, Schema>,
    ) -> Box<dyn Processor> {
        todo!()
    }
}

macro_rules! test_ports {
    ($id:ident, $out_ports:expr, $in_ports:expr, $from_port:expr, $to_port:expr, $expect:expr) => {
        #[test]
        fn $id() {
            let src = DynPortsSourceFactory::new($out_ports);
            let proc = DynPortsProcessorFactory::new($in_ports, vec![DEFAULT_PORT_HANDLE]);

            let source_handle = NodeHandle::new(None, 1);
            let proc_handle = NodeHandle::new(Some(1), 1);

            let mut dag = Dag::new();

            dag.add_node(NodeType::Source(Arc::new(src)), source_handle);
            dag.add_node(NodeType::Processor(Arc::new(proc)), proc_handle);

            let res = dag.connect(
                Endpoint::new(source_handle, $from_port),
                Endpoint::new(proc_handle, $to_port),
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

// #[test]
// fn test_dag_merge() {
//     let src = DynPortsSourceFactory::new(vec![DEFAULT_PORT_HANDLE]);
//     let proc = DynPortsProcessorFactory::new(vec![DEFAULT_PORT_HANDLE], vec![DEFAULT_PORT_HANDLE]);
//
//     let mut dag = Dag::new();
//
//     let source_handle = NodeHandle::new(None, 1);
//     let proc_handle = NodeHandle::new(Some(1), 1);
//
//     dag.add_node(NodeType::Source(Arc::new(src)), source_handle);
//     dag.add_node(NodeType::Processor(Arc::new(proc)), proc_handle);
//
//     let mut new_dag: Dag = Dag::new();
//     new_dag.merge("test".to_string(), dag);
//
//     let res = new_dag.connect(
//         Endpoint::new(source_handle, DEFAULT_PORT_HANDLE),
//         Endpoint::new(proc_handle, DEFAULT_PORT_HANDLE),
//     );
//     assert!(res.is_err());
//
//     let res = new_dag.connect(
//         Endpoint::new("test_1".to_string(), DEFAULT_PORT_HANDLE),
//         Endpoint::new("test_2".to_string(), DEFAULT_PORT_HANDLE),
//     );
//     assert!(res.is_ok())
// }
