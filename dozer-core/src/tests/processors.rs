use std::collections::HashMap;

use dozer_types::{errors::internal::BoxedError, types::Schema};

use crate::{
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory},
    DEFAULT_PORT_HANDLE,
};

use super::app::NoneContext;

#[derive(Debug)]
pub struct ConnectivityTestProcessorFactory;

impl ProcessorFactory<NoneContext> for ConnectivityTestProcessorFactory {
    fn type_name(&self) -> String {
        "ConnectivityTest".to_owned()
    }
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            DEFAULT_PORT_HANDLE,
            OutputPortType::Stateless,
        )]
    }

    fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        _input_schemas: &HashMap<PortHandle, (Schema, NoneContext)>,
    ) -> Result<(Schema, NoneContext), BoxedError> {
        unimplemented!(
            "This struct is for connectivity test, only input and output ports are defined"
        )
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        unimplemented!(
            "This struct is for connectivity test, only input and output ports are defined"
        )
    }
}

#[derive(Debug)]
pub struct NoInputPortProcessorFactory;

impl ProcessorFactory<NoneContext> for NoInputPortProcessorFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![]
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            DEFAULT_PORT_HANDLE,
            OutputPortType::Stateless,
        )]
    }

    fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        _input_schemas: &HashMap<PortHandle, (Schema, NoneContext)>,
    ) -> Result<(Schema, NoneContext), BoxedError> {
        unimplemented!(
            "This struct is for connectivity test, only input and output ports are defined"
        )
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        unimplemented!(
            "This struct is for connectivity test, only input and output ports are defined"
        )
    }

    fn type_name(&self) -> String {
        "NoInput".to_owned()
    }
}
