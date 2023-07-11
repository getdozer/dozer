use crate::node::{
    OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory, Source, SourceFactory,
};
use crate::tests::app::NoneContext;
use crate::{Dag, Endpoint, DEFAULT_PORT_HANDLE};
use dozer_types::errors::internal::BoxedError;
use dozer_types::{node::NodeHandle, types::Schema};
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

impl SourceFactory<NoneContext> for DynPortsSourceFactory {
    fn get_output_schema(&self, _port: &PortHandle) -> Result<(Schema, NoneContext), BoxedError> {
        todo!()
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        self.output_ports
            .iter()
            .map(|p| OutputPortDef::new(*p, OutputPortType::Stateless))
            .collect()
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, BoxedError> {
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

impl ProcessorFactory<NoneContext> for DynPortsProcessorFactory {
    fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        _input_schemas: &HashMap<PortHandle, (Schema, NoneContext)>,
    ) -> Result<(Schema, NoneContext), BoxedError> {
        todo!()
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        self.input_ports.clone()
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        self.output_ports
            .iter()
            .map(|p| OutputPortDef::new(*p, OutputPortType::Stateless))
            .collect()
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        todo!()
    }

    fn type_name(&self) -> String {
        "DynPorts".to_owned()
    }

    fn id(&self) -> String {
        "DynPorts".to_owned()
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

            dag.add_source(source_handle.clone(), Arc::new(src));
            dag.add_processor(proc_handle.clone(), Arc::new(proc));

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
