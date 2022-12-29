use crate::dag::dag::{Dag, Endpoint, NodeType, DEFAULT_PORT_HANDLE};
use crate::dag::errors::ExecutionError;
use crate::dag::node::{
    NodeHandle, OutputPortDef, OutputPortDefOptions, PortHandle, Processor, ProcessorFactory,
    Source, SourceFactory,
};
use dozer_types::types::Schema;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
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

    fn prepare(&self, _output_schemas: HashMap<PortHandle, Schema>) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, ExecutionError> {
        todo!()
    }
}

#[derive(Debug)]
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

    fn prepare(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Processor>, ExecutionError> {
        todo!()
    }
}

macro_rules! test_ports {
    ($id:ident, $out_ports:expr, $in_ports:expr, $from_port:expr, $to_port:expr, $expect:expr) => {
        #[test]
        fn $id() {
            let src = DynPortsSourceFactory::new($out_ports);
            let proc = DynPortsProcessorFactory::new($in_ports, vec![DEFAULT_PORT_HANDLE]);

            let source_handle = NodeHandle::new(None, 1.to_string());
            let proc_handle = NodeHandle::new(Some(1), 1.to_string());

            let mut dag = Dag::new();

            dag.add_node(NodeType::Source(Arc::new(src)), source_handle.clone());
            dag.add_node(NodeType::Processor(Arc::new(proc)), proc_handle.clone());

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
