use crate::{
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory},
    DEFAULT_PORT_HANDLE,
};

use super::app::NoneContext;

#[derive(Debug)]
pub struct ConnectivityTestProcessorFactory;

impl ProcessorFactory<NoneContext> for ConnectivityTestProcessorFactory {
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
        _input_schemas: &std::collections::HashMap<
            PortHandle,
            (dozer_types::types::Schema, NoneContext),
        >,
    ) -> Result<(dozer_types::types::Schema, NoneContext), crate::errors::ExecutionError> {
        unimplemented!(
            "This struct is for connectivity test, only input and output ports are defined"
        )
    }

    fn build(
        &self,
        _input_schemas: std::collections::HashMap<PortHandle, dozer_types::types::Schema>,
        _output_schemas: std::collections::HashMap<PortHandle, dozer_types::types::Schema>,
    ) -> Result<Box<dyn Processor>, crate::errors::ExecutionError> {
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
        _input_schemas: &std::collections::HashMap<
            PortHandle,
            (dozer_types::types::Schema, NoneContext),
        >,
    ) -> Result<(dozer_types::types::Schema, NoneContext), crate::errors::ExecutionError> {
        unimplemented!(
            "This struct is for connectivity test, only input and output ports are defined"
        )
    }

    fn build(
        &self,
        _input_schemas: std::collections::HashMap<PortHandle, dozer_types::types::Schema>,
        _output_schemas: std::collections::HashMap<PortHandle, dozer_types::types::Schema>,
    ) -> Result<Box<dyn Processor>, crate::errors::ExecutionError> {
        unimplemented!(
            "This struct is for connectivity test, only input and output ports are defined"
        )
    }
}
